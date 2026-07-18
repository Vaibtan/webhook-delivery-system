// Package ratelimit implements a per-subscription token-bucket rate limiter.
// Mutex-only (no sync/atomic) — every read/write happens under Allow()'s lock,
// so atomics would be redundant. Fixed-point (×1000) avoids float drift in the
// refill math.
package ratelimit

import (
	"sync"
	"time"
)

const scale = 1000 // fixed-point factor

// TokenBucket is a single subscription's bucket. Construct via NewTokenBucket.
type TokenBucket struct {
	mu         sync.Mutex
	tokens     int64 // current tokens × scale
	maxTokens  int64 // capacity × scale
	refillRate int64 // tokens/sec × scale
	lastRefill time.Time
	now        func() time.Time // injectable clock (tests)
}

// NewTokenBucket creates a bucket with the validated steady-state rate and burst
// capacity.
func NewTokenBucket(ratePerSec, burst int) *TokenBucket {
	return &TokenBucket{
		tokens:     int64(burst) * scale,
		maxTokens:  int64(burst) * scale,
		refillRate: int64(ratePerSec) * scale,
		lastRefill: time.Now(),
		now:        time.Now,
	}
}

// Allow consumes one token if available.
func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	if tb.tokens >= scale {
		tb.tokens -= scale
		return true
	}
	return false
}

// refill adds tokens proportional to elapsed time, capped at capacity.
func (tb *TokenBucket) refill() {
	now := tb.now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	tb.tokens = min(tb.maxTokens, tb.tokens+int64(elapsed*float64(tb.refillRate)))
	tb.lastRefill = now
}
