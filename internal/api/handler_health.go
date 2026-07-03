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

// handleReady is the readiness probe. In Slice 1 it mirrors dependency
// reachability; Slice 4 upgrades it to require a running worker pool. Public.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), healthProbeTimeout)
	defer cancel()

	checks := map[string]string{
		"database": probe(ctx, s.opts.PingDB),
		"redis":    probe(ctx, s.opts.PingRedis),
	}
	if s.opts.WorkerReady != nil {
		checks["worker"] = boolCheck(s.opts.WorkerReady())
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

// probe runs a ping function (nil-safe) and renders an "ok" / error string.
func probe(ctx context.Context, fn func(ctx context.Context) error) string {
	if fn == nil {
		return "unconfigured"
	}
	if err := fn(ctx); err != nil {
		return err.Error()
	}
	return "ok"
}

func boolCheck(ok bool) string {
	if ok {
		return "ok"
	}
	return "not_running"
}
