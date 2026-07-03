package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// brpopTimeout bounds each blocking dequeue so the loop can re-check ctx.Done()
// for prompt shutdown.
const brpopTimeout = 2 * time.Second

// Pool is the bounded goroutine pool that drains webhook:queue and delivers.
type Pool struct {
	queue        domain.TaskQueue
	deliverer    domain.Deliverer
	concurrency  int
	drainTimeout time.Duration
	running      atomic.Bool
}

// NewPool constructs a Pool. drainTimeout is the per-delivery hard deadline that
// in-flight work gets after SIGTERM (via context.WithoutCancel).
func NewPool(queue domain.TaskQueue, deliverer domain.Deliverer, concurrency int, drainTimeout time.Duration) *Pool {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Pool{
		queue:        queue,
		deliverer:    deliverer,
		concurrency:  concurrency,
		drainTimeout: drainTimeout,
	}
}

// Running reports whether the pool's dequeue loop is active (backs /ready).
func (p *Pool) Running() bool { return p.running.Load() }

// Start runs the dequeue/deliver loop until ctx is cancelled, then drains
// in-flight deliveries within drainTimeout. Two correctness rules (plan §1):
//
//  1. A single failed delivery must NOT tear down the pool — we use a plain
//     errgroup.Group (no derived cancel-on-error ctx) and never propagate a
//     delivery error.
//  2. Graceful drain ≠ cancellation — SIGTERM cancels ctx to stop *dequeuing*,
//     but each in-flight delivery runs on a context.WithoutCancel-derived
//     context with its own deadline, so it finishes instead of aborting.
func (p *Pool) Start(ctx context.Context) error {
	p.running.Store(true)
	defer p.running.Store(false)

	g := new(errgroup.Group) // plain Group: no shared ctx to cancel on error
	g.SetLimit(p.concurrency)

	for ctx.Err() == nil {
		logID, err := p.queue.Dequeue(ctx, brpopTimeout)
		if err != nil {
			if errors.Is(err, domain.ErrQueueEmpty) || ctx.Err() != nil {
				continue
			}
			slog.Error("pool: dequeue failed", "error", err)
			continue
		}

		g.Go(func() error { // blocks here when the concurrency limit is hit
			// Drain context: detach cancellation (survive SIGTERM) but keep a hard
			// deadline so shutdown still bounds in-flight work.
			dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.drainTimeout)
			defer cancel()
			if err := p.deliverer.Process(dctx, logID); err != nil {
				// Must stay nil-returning (rule 1). Log and move on.
				slog.Error("pool: deliver unexpected error", "delivery_id", logID, "error", err)
			}
			return nil
		})
	}
	return g.Wait() // drains all in-flight within their deadlines
}
