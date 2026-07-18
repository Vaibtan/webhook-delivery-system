// Package semaphore is a counting semaphore with non-blocking acquisition, used
// to cap in-flight deliveries per subscription (noisy-neighbor protection). The
// non-blocking TryAcquire lets a worker requeue instead of parking, avoiding
// head-of-line starvation.
package semaphore

// Semaphore is a bounded, concurrency-safe permit holder.
type Semaphore struct {
	tokens chan struct{}
}

// New constructs a semaphore allowing n concurrent holders (n>=1).
func New(n int) *Semaphore {
	return &Semaphore{tokens: make(chan struct{}, n)}
}

// TryAcquire takes a permit without blocking, returning false if at capacity.
func (s *Semaphore) TryAcquire() bool {
	select {
	case s.tokens <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release returns a permit. Safe to call only after a successful TryAcquire.
func (s *Semaphore) Release() {
	<-s.tokens
}
