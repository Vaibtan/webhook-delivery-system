package lifecycle

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeHTTPServer struct {
	started         chan struct{}
	stopped         chan struct{}
	serveErr        error
	shutdownErr     error
	stopOnce        sync.Once
	mu              sync.Mutex
	shutdownCalled  bool
	shutdownBounded bool
}

func newFakeHTTPServer() *fakeHTTPServer {
	return &fakeHTTPServer{started: make(chan struct{}), stopped: make(chan struct{})}
}

func (s *fakeHTTPServer) ListenAndServe() error {
	close(s.started)
	if s.serveErr != nil {
		return s.serveErr
	}
	<-s.stopped
	return http.ErrServerClosed
}

func (s *fakeHTTPServer) Shutdown(ctx context.Context) error {
	_, bounded := ctx.Deadline()
	s.mu.Lock()
	s.shutdownCalled = true
	s.shutdownBounded = bounded
	s.mu.Unlock()
	s.stopOnce.Do(func() { close(s.stopped) })
	return s.shutdownErr
}

func (s *fakeHTTPServer) shutdownState() (called, bounded bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shutdownCalled, s.shutdownBounded
}

func TestParentCancellationShutsDownHTTPAndWaitsForDrain(t *testing.T) {
	server := newFakeHTTPServer()
	processCancelled := make(chan struct{})
	releaseDrain := make(chan struct{})
	supervisor := New(server, time.Second, Process{
		Name: "draining worker",
		Run: func(ctx context.Context) error {
			<-ctx.Done()
			close(processCancelled)
			<-releaseDrain
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	<-server.started
	cancel()
	<-processCancelled

	select {
	case err := <-done:
		t.Fatalf("supervisor returned before the worker drained: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseDrain)
	require.NoError(t, <-done)
	called, bounded := server.shutdownState()
	assert.True(t, called)
	assert.True(t, bounded)
}

func TestHTTPFailureCancelsProcesses(t *testing.T) {
	serveErr := errors.New("listen failed")
	server := newFakeHTTPServer()
	server.serveErr = serveErr
	processCancelled := make(chan struct{})
	supervisor := New(server, time.Second, Process{
		Name: "worker",
		Run: func(ctx context.Context) error {
			<-ctx.Done()
			close(processCancelled)
			return nil
		},
	})

	err := supervisor.Run(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, serveErr)
	assert.Contains(t, err.Error(), "http serve")
	assertClosed(t, processCancelled)
	called, _ := server.shutdownState()
	assert.True(t, called)
}

func TestProcessFailureShutsDownHTTP(t *testing.T) {
	processErr := errors.New("worker failed")
	server := newFakeHTTPServer()
	supervisor := New(server, time.Second, Process{
		Name: "worker",
		Run:  func(context.Context) error { return processErr },
	})

	err := supervisor.Run(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, processErr)
	assert.Contains(t, err.Error(), "process worker")
	called, _ := server.shutdownState()
	assert.True(t, called)
}

func TestServerClosedCompletesCleanly(t *testing.T) {
	server := newFakeHTTPServer()
	server.serveErr = http.ErrServerClosed

	require.NoError(t, New(server, time.Second).Run(context.Background()))
	called, _ := server.shutdownState()
	assert.True(t, called)
}

func TestEarlyCleanProcessExitCancelsPeers(t *testing.T) {
	server := newFakeHTTPServer()
	peerCancelled := make(chan struct{})
	supervisor := New(
		server,
		time.Second,
		Process{Name: "finished", Run: func(context.Context) error { return nil }},
		Process{
			Name: "peer",
			Run: func(ctx context.Context) error {
				<-ctx.Done()
				close(peerCancelled)
				return nil
			},
		},
	)

	require.NoError(t, supervisor.Run(context.Background()))
	assertClosed(t, peerCancelled)
}

func assertClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	default:
		t.Fatal("channel was not closed")
	}
}
