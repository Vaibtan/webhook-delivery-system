package api

import (
	"context"
	"net/http"
	"time"
)

const healthProbeTimeout = 2 * time.Second

// healthResponse is the body of /health and /ready.
type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// handleHealth is the liveness probe: it pings Postgres and Redis and reports
// 200 only when both are reachable, 503 otherwise. Public (no auth).
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), healthProbeTimeout)
	defer cancel()

	checks := map[string]string{
		"database": probe(ctx, s.opts.PingDB),
		"redis":    probe(ctx, s.opts.PingRedis),
	}

	status := http.StatusOK
	overall := "ok"
	for _, v := range checks {
		if v != "ok" {
			status = http.StatusServiceUnavailable
			overall = "degraded"
			break
		}
	}
	writeJSON(w, status, healthResponse{Status: overall, Checks: checks})
}

// handleReady requires reachable dependencies and a running worker pool.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), healthProbeTimeout)
	defer cancel()

	checks := map[string]string{
		"database": probe(ctx, s.opts.PingDB),
		"redis":    probe(ctx, s.opts.PingRedis),
	}
	if s.opts.WorkerReady() {
		checks["worker"] = "ok"
	} else {
		checks["worker"] = "not_running"
	}

	status := http.StatusOK
	overall := "ready"
	for _, v := range checks {
		if v != "ok" {
			status = http.StatusServiceUnavailable
			overall = "not_ready"
			break
		}
	}
	writeJSON(w, status, healthResponse{Status: overall, Checks: checks})
}

// probe renders a dependency ping as "ok" or its error.
func probe(ctx context.Context, fn func(ctx context.Context) error) string {
	if err := fn(ctx); err != nil {
		return err.Error()
	}
	return "ok"
}
