// Package breaker implements a per-target-URL circuit breaker with a
// Closed→Open→Half-Open FSM. A generation counter makes the half-open probe
// authoritative: a request that began in one era and finishes in another carries
// a stale token and cannot spuriously change state.
package breaker

import (
	"sync/atomic"
	"time"
)

type state int32

const (
	stateClosed   state = iota // 0
	stateOpen                  // 1
	stateHalfOpen              // 2
)

// CircuitBreaker is safe for concurrent use.
type CircuitBreaker struct {
	state        atomic.Int32
	failures     atomic.Int32
	lastOpenedAt atomic.Int64 // UnixNano
	generation   atomic.Int64 // bumped on every Open and every Half-Open probe issue
	probing      atomic.Bool  // a probe is in flight
	threshold    int32
	resetTimeout time.Duration
	now          func() time.Time // injectable clock (tests); set once at construction
}

// New creates a Closed breaker that opens after `threshold` consecutive failures
// and admits one probe after `resetTimeout`.
func New(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    int32(threshold),
		resetTimeout: resetTimeout,
		now:          time.Now,
	}
}

// Allow reports whether a request may proceed and returns a generation TOKEN the
// caller MUST hand back to RecordSuccess/RecordFailure. A result whose token no
// longer matches the current generation is stale and is ignored.
func (cb *CircuitBreaker) Allow() (bool, int64) {
	switch state(cb.state.Load()) {
	case stateClosed:
		return true, cb.generation.Load()
	case stateOpen:
		elapsed := cb.now().Sub(time.Unix(0, cb.lastOpenedAt.Load()))
		if elapsed > cb.resetTimeout && cb.probing.CompareAndSwap(false, true) {
			cb.state.Store(int32(stateHalfOpen))
			return true, cb.generation.Add(1) // exactly one probe, fresh generation
		}
		return false, 0
	case stateHalfOpen:
		return false, 0 // block everyone but the single in-flight probe
	}
	return false, 0
}

// RecordSuccess reports a successful request for the given token.
func (cb *CircuitBreaker) RecordSuccess(token int64) {
	if cb.generation.Load() != token {
		return // stale result from a previous generation — ignore
	}
	if state(cb.state.Load()) == stateHalfOpen { // the probe succeeded
		cb.state.CompareAndSwap(int32(stateHalfOpen), int32(stateClosed))
		cb.probing.Store(false)
	}
	cb.failures.Store(0)
}

// RecordFailure reports a failed request for the given token.
func (cb *CircuitBreaker) RecordFailure(token int64) {
	if cb.generation.Load() != token {
		return // stale result — ignore
	}
	if state(cb.state.Load()) == stateHalfOpen { // probe failed: reopen, fresh cooldown
		cb.lastOpenedAt.Store(cb.now().UnixNano())
		cb.state.Store(int32(stateOpen))
		cb.generation.Add(1)
		cb.probing.Store(false)
		return
	}
	if cb.failures.Add(1) >= cb.threshold {
		cb.lastOpenedAt.Store(cb.now().UnixNano())
		if cb.state.CompareAndSwap(int32(stateClosed), int32(stateOpen)) {
			cb.generation.Add(1) // invalidate all closed-era in-flight tokens
		}
	}
}

// State returns the breaker state as a string for the /monitor endpoint.
func (cb *CircuitBreaker) State() string {
	switch state(cb.state.Load()) {
	case stateOpen:
		return "open"
	case stateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}
