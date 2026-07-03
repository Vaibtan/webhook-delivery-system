package breaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func clockAt(t *time.Time) func() time.Time { return func() time.Time { return *t } }

func TestOpensAfterThreshold(t *testing.T) {
	cb := New(3, 30*time.Second)
	ok, tok := cb.Allow()
	require.True(t, ok)
	assert.Equal(t, "closed", cb.State())

	cb.RecordFailure(tok)
	cb.RecordFailure(tok)
	assert.Equal(t, "closed", cb.State(), "still closed below threshold")
	cb.RecordFailure(tok)
	assert.Equal(t, "open", cb.State(), "opens at threshold")

	ok, _ = cb.Allow()
	assert.False(t, ok, "Open breaker denies before reset timeout")
}

func TestHalfOpenSingleProbeThenClose(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cb := New(2, 30*time.Second)
	cb.now = clockAt(&now)

	_, tok := cb.Allow()
	cb.RecordFailure(tok)
	cb.RecordFailure(tok)
	require.Equal(t, "open", cb.State())

	// Before reset timeout: denied.
	ok, _ := cb.Allow()
	assert.False(t, ok)

	// After reset timeout: exactly one probe admitted.
	now = now.Add(31 * time.Second)
	ok, probeTok := cb.Allow()
	require.True(t, ok, "first caller after reset gets the probe")
	assert.Equal(t, "half-open", cb.State())
	ok2, _ := cb.Allow()
	assert.False(t, ok2, "second caller blocked while probe in flight")

	// Probe succeeds → closed.
	cb.RecordSuccess(probeTok)
	assert.Equal(t, "closed", cb.State())
	ok3, _ := cb.Allow()
	assert.True(t, ok3, "closed again admits traffic")
}

func TestProbeFailureReopens(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cb := New(1, 10*time.Second)
	cb.now = clockAt(&now)

	_, tok := cb.Allow()
	cb.RecordFailure(tok)
	require.Equal(t, "open", cb.State())

	now = now.Add(11 * time.Second)
	ok, probeTok := cb.Allow()
	require.True(t, ok)
	require.Equal(t, "half-open", cb.State())

	cb.RecordFailure(probeTok)
	assert.Equal(t, "open", cb.State(), "failed probe reopens")
}

// The key correctness claim: a success carrying a STALE (closed-era) token must
// NOT close an Open breaker.
func TestStaleClosedEraSuccessDoesNotCloseOpen(t *testing.T) {
	cb := New(3, 30*time.Second)
	_, staleTok := cb.Allow() // token from the closed era
	cb.RecordFailure(staleTok)
	cb.RecordFailure(staleTok)
	cb.RecordFailure(staleTok)
	require.Equal(t, "open", cb.State())

	// A late-arriving success from the closed era.
	cb.RecordSuccess(staleTok)
	assert.Equal(t, "open", cb.State(), "stale closed-era success must not close the breaker")

	ok, _ := cb.Allow()
	assert.False(t, ok, "breaker remains open and denying")
}

func TestStaleFailureIgnored(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cb := New(1, 10*time.Second)
	cb.now = clockAt(&now)

	_, tok := cb.Allow()
	cb.RecordFailure(tok) // opens, gen advances
	require.Equal(t, "open", cb.State())

	now = now.Add(11 * time.Second)
	_, probeTok := cb.Allow() // half-open probe, gen advances again
	require.Equal(t, "half-open", cb.State())

	// The original stale token must not affect the in-flight probe.
	cb.RecordFailure(tok)
	assert.Equal(t, "half-open", cb.State(), "stale token ignored; probe still in flight")
	cb.RecordSuccess(probeTok)
	assert.Equal(t, "closed", cb.State())
}
