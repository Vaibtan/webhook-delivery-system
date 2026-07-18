package ratelimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTokenBucketBurstThenBlock(t *testing.T) {
	tb := NewTokenBucket(10, 5) // 10/sec, burst 5
	// Freeze time so no refill happens mid-burst (seed lastRefill to the clock).
	frozen := time.Unix(1_700_000_000, 0)
	tb.now = func() time.Time { return frozen }
	tb.lastRefill = frozen

	for i := 0; i < 5; i++ {
		assert.Truef(t, tb.Allow(), "burst token %d should be allowed", i+1)
	}
	assert.False(t, tb.Allow(), "6th request must be blocked (burst exhausted)")
}

func TestTokenBucketRefillsOverTime(t *testing.T) {
	tb := NewTokenBucket(10, 5) // 10 tokens/sec
	cur := time.Unix(1_700_000_000, 0)
	tb.now = func() time.Time { return cur }
	tb.lastRefill = cur

	for i := 0; i < 5; i++ {
		assert.True(t, tb.Allow())
	}
	assert.False(t, tb.Allow(), "exhausted")

	// Advance 1s → +10 tokens (capped at burst 5).
	cur = cur.Add(time.Second)
	for i := 0; i < 5; i++ {
		assert.Truef(t, tb.Allow(), "refilled token %d", i+1)
	}
	assert.False(t, tb.Allow(), "capped at burst, exhausted again")

	// Advance 200ms → +2 tokens.
	cur = cur.Add(200 * time.Millisecond)
	assert.True(t, tb.Allow())
	assert.True(t, tb.Allow())
	assert.False(t, tb.Allow())
}
