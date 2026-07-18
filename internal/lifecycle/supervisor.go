// Package lifecycle coordinates the service's long-lived processes and
// graceful HTTP shutdown.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"
)

// HTTPServer is the lifecycle surface used from net/http.Server.
type HTTPServer interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

// Process is one long-lived service component.
type Process struct {
	Name string
	Run  func(context.Context) error
}

// Supervisor owns cancellation, shutdown, and draining for an HTTP server and
// its background processes.
type Supervisor struct {
	server          HTTPServer
	shutdownTimeout time.Duration
	processes       []Process
}

// New constructs a Supervisor around already-composed service components.
func New(server HTTPServer, shutdownTimeout time.Duration, processes ...Process) *Supervisor {
	return &Supervisor{
		server:          server,
		shutdownTimeout: shutdownTimeout,
		processes:       processes,
	}
}

// Run blocks until the parent is cancelled or any component exits. It then
// cancels peers, shuts down HTTP, and waits for every process to drain.
func (s *Supervisor) Run(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	g, groupCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer cancel()
		err := s.server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http serve: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		<-groupCtx.Done()
		shutdownCtx, stopShutdown := context.WithTimeout(
			context.WithoutCancel(groupCtx),
			s.shutdownTimeout,
		)
		defer stopShutdown()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		return nil
	})

	for _, process := range s.processes {
		g.Go(func() error {
			defer cancel()
			slog.Info("service process starting", "process", process.Name)
			if err := process.Run(groupCtx); err != nil {
				return fmt.Errorf("process %s: %w", process.Name, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	return nil
}
