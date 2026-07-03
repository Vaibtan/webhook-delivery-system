package registry

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetCreatesOnceAndReuses(t *testing.T) {
	var created atomic.Int32
	r := New(func(key string) string {
		created.Add(1)
		return "v:" + key
	}, time.Hour)

	assert.Equal(t, "v:a", r.Get("a"))
	assert.Equal(t, "v:a", r.Get("a"))
	assert.Equal(t, "v:b", r.Get("b"))
	assert.Equal(t, int32(2), created.Load(), "create called once per distinct key")
	assert.Equal(t, 2, r.Len())
}

func TestRemove(t *testing.T) {
	r := New(func(key string) int { return 1 }, time.Hour)
	r.Get("a")
	r.Get("b")
	r.Remove("a")
	assert.Equal(t, 1, r.Len())
}

func TestSweepEvictsIdle(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	r := New(func(key string) int { return 1 }, time.Minute)
	r.now = func() time.Time { return now }

	r.Get("fresh")
	r.Get("stale")
	// Advance past idleTTL, then touch only "fresh".
	now = now.Add(2 * time.Minute)
	r.Get("fresh")

	evicted := r.Sweep()
	assert.Equal(t, 1, evicted, "only the stale entry is swept")
	assert.Equal(t, 1, r.Len())
	assert.Equal(t, 1, r.Get("fresh")) // fresh survived
}
