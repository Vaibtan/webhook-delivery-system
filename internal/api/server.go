// Package api wires the net/http ServeMux, middleware chain, and HTTP handlers
// for the webhook delivery service. All routes are versioned under /api/v1.
package api

import (
	"context"
	"expvar"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/metrics"
)

// MetricsProvider exposes uptime + latency for /monitor (satisfied by
// *metrics.Collector).
type MetricsProvider interface {
	Uptime() time.Duration
	Latency() metrics.HistogramSnapshot
}

// defaultMaxBodyBytes is the global request body cap (defence against OOM/DoS).
// The ingest handler applies a stricter 64KB limit on top of this.
const defaultMaxBodyBytes = 1 << 20 // 1 MiB

// The HTTP layer depends on these narrow, consumer-defined views of the
// persistence and queue adapters rather than the full domain ports — each
// declares only the methods the handlers actually call. The concrete
// store/queue adapters satisfy them implicitly.
type (
	subscriptionStore interface {
		Create(ctx context.Context, sub *domain.Subscription) error
		GetByID(ctx context.Context, id string) (*domain.Subscription, error)
		List(ctx context.Context, limit int, after *domain.Cursor) (domain.Page[*domain.Subscription], error)
		Update(ctx context.Context, sub *domain.Subscription) error
		Delete(ctx context.Context, id string) error
		RotateSecret(ctx context.Context, id, newSecret string) (*domain.Subscription, error)
	}

	deliveryIngestStore interface {
		GetByID(ctx context.Context, id string) (*domain.DeliveryLog, error)
		IngestPending(ctx context.Context, p domain.IngestParams) (domain.IngestResult, error)
	}

	deliveryStatusStore interface {
		ListByWebhookID(ctx context.Context, webhookID string) ([]*domain.DeliveryLog, error)
		ListBySubscription(ctx context.Context, subscriptionID string, limit int) ([]*domain.DeliveryLog, error)
		CountByStatusSince(ctx context.Context, since time.Time) (domain.StatusCounts, error)
	}

	deliveryDLQStore interface {
		AckDLQ(ctx context.Context, subscriptionID, webhookID string) (claimedID string, ok bool, err error)
		ReplayDLQ(ctx context.Context, subscriptionID, webhookID string) (claimedID, newID string, ok bool, err error)
	}

	// taskQueue is what the handlers use of the work queue: enqueue (ingest/replay)
	// and depth (/monitor).
	taskQueue interface {
		Enqueue(ctx context.Context, deliveryLogID string) error
		Depth(ctx context.Context) (int64, error)
	}

	// deadLetterQueue is what the handlers use of the DLQ LIST index: LREM on
	// ack/replay and depth (/monitor).
	deadLetterQueue interface {
		Remove(ctx context.Context, deliveryLogID string) error
		Depth(ctx context.Context) (int64, error)
	}
)

// Options holds the collaborators the HTTP layer needs, injected as narrow
// consumer-defined interfaces so this package stays free of infrastructure
// imports. All fields are required unless marked OPTIONAL.
type Options struct {
	// Liveness probes for /health.
	PingDB    func(ctx context.Context) error
	PingRedis func(ctx context.Context) error
	// WorkerReady reports whether the worker pool is running.
	WorkerReady func() bool

	// Persistence.
	Subscriptions  subscriptionStore
	DeliveryIngest deliveryIngestStore
	DeliveryStatus deliveryStatusStore
	DeliveryDLQ    deliveryDLQStore

	// Delivery pipeline.
	SyncDelivery bool             // make the first attempt in-request; retries remain async
	Deliverer    domain.Deliverer // used by the sync path
	TaskQueue    taskQueue        // async enqueue + queue depth
	DLQ          deadLetterQueue  // dead-letter LIST index (replay/ack LREM, depth)

	// Per-subscription admission.
	RateLimitAllow func(subscriptionID string) bool

	// Observability.
	Metrics       MetricsProvider
	BreakerStates func() map[string]string
	EnablePprof   bool

	// Policy / config.
	AdminAPIKey          string
	AllowHTTPURLs        bool
	CORSAllowedOrigins   []string
	SignatureDriftWindow time.Duration
	SecretGraceWindow    time.Duration
}

// Server holds the routed mux and its dependencies.
type Server struct {
	opts Options
	mux  *http.ServeMux
}

// NewServer builds the routed HTTP server.
func NewServer(opts Options) *Server {
	s := &Server{opts: opts, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Handler returns the root http.Handler with the global middleware chain applied
// (outermost first): request-id → logging → recovery → CORS → body-size limit.
func (s *Server) Handler() http.Handler {
	return chain(s.mux,
		requestIDMiddleware,
		loggingMiddleware,
		recoveryMiddleware,
		corsMiddleware(s.opts.CORSAllowedOrigins),
		maxBytesMiddleware(defaultMaxBodyBytes),
	)
}

// admin wraps a handler with the fail-closed bearer-token middleware.
func (s *Server) admin(h http.HandlerFunc) http.Handler {
	return adminAuthMiddleware(s.opts.AdminAPIKey)(h)
}

// routes registers all endpoints. Public probes are mounted directly; the
// management group is wrapped with admin auth.
func (s *Server) routes() {
	// Public liveness/readiness.
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/v1/ready", s.handleReady)

	// Public API documentation.
	s.mux.HandleFunc("GET /api/v1/openapi.json", s.handleOpenAPISpec)
	s.mux.HandleFunc("GET /api/v1/docs", s.handleDocs)

	// Subscription management (admin auth).
	s.mux.Handle("POST /api/v1/subscriptions", s.admin(s.handleCreateSubscription))
	s.mux.Handle("GET /api/v1/subscriptions", s.admin(s.handleListSubscriptions))
	s.mux.Handle("GET /api/v1/subscriptions/{id}", s.admin(s.handleGetSubscription))
	s.mux.Handle("PUT /api/v1/subscriptions/{id}", s.admin(s.handleUpdateSubscription))
	s.mux.Handle("DELETE /api/v1/subscriptions/{id}", s.admin(s.handleDeleteSubscription))
	s.mux.Handle("GET /api/v1/subscriptions/{id}/attempts", s.admin(s.handleListAttempts))

	// Secret rotation (admin auth).
	s.mux.Handle("POST /api/v1/subscriptions/{id}/rotate-secret", s.admin(s.handleRotateSecret))

	// DLQ operations (admin auth).
	s.mux.Handle("POST /api/v1/subscriptions/{id}/replay/{webhook_id}", s.admin(s.handleReplay))
	s.mux.Handle("POST /api/v1/subscriptions/{id}/dlq/{webhook_id}/ack", s.admin(s.handleAckDLQ))

	// Ingest — public; the per-subscription HMAC is the authentication.
	s.mux.HandleFunc("POST /api/v1/ingest/{id}", s.handleIngest)

	// Status / metrics (admin auth). The two-segment metrics path is more
	// specific than {webhook_id}, so ServeMux routes them unambiguously.
	s.mux.Handle("GET /api/v1/status/metrics/summary", s.admin(s.handleMetricsSummary))
	s.mux.Handle("GET /api/v1/status/{webhook_id}", s.admin(s.handleWebhookStatus))

	// Observability (admin auth).
	s.mux.Handle("GET /api/v1/monitor", s.admin(s.handleMonitor))
	s.mux.Handle("GET /api/v1/debug/vars", s.admin(expvar.Handler().ServeHTTP))

	// pprof — admin auth AND env-gated (defence in depth; profiles are sensitive).
	if s.opts.EnablePprof {
		pp := http.NewServeMux()
		pp.HandleFunc("/debug/pprof/", pprof.Index)
		pp.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pp.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pp.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pp.HandleFunc("/debug/pprof/trace", pprof.Trace)
		stripped := http.StripPrefix("/api/v1", pp)
		s.mux.Handle("/api/v1/debug/pprof/", s.admin(func(w http.ResponseWriter, r *http.Request) {
			stripped.ServeHTTP(w, r)
		}))
	}
}
