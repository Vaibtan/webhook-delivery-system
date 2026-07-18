package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/Vaibtan/webhook-delivery-system/internal/infra/idgen"
)

type contextKey int

const (
	ctxKeyRequestID contextKey = iota
	ctxKeyLogger
)

const requestIDHeader = "X-Request-ID"

// middleware is the standard wrapper signature.
type middleware func(http.Handler) http.Handler

// chain applies middlewares so that the first listed is the outermost.
func chain(h http.Handler, mws ...middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// requestIDMiddleware assigns/propagates a request ID and attaches a
// request-scoped slog logger to the context (carried across API + worker layers).
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = idgen.MustUUIDv4()
		}
		w.Header().Set(requestIDHeader, id)
		logger := slog.Default().With("request_id", id)
		ctx := context.WithValue(r.Context(), ctxKeyRequestID, id)
		ctx = context.WithValue(ctx, ctxKeyLogger, logger)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// loggerFrom returns the request-scoped logger, falling back to the default.
func loggerFrom(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKeyLogger).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

// statusRecorder captures the response status code for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.wrote {
		return
	}
	s.status = code
	s.wrote = true
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if !s.wrote {
		s.status = http.StatusOK
		s.wrote = true
	}
	return s.ResponseWriter.Write(b)
}

// loggingMiddleware emits one structured log line per request.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		loggerFrom(r.Context()).Info("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"remote", r.RemoteAddr,
		)
	})
}

// recoveryMiddleware converts a panic into a 500 instead of crashing the server.
func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				loggerFrom(r.Context()).Error("panic recovered", "panic", rec, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// maxBytesMiddleware caps request body size globally (defence against OOM/DoS).
// The ingest handler applies its own stricter 64KB limit on top of this.
func maxBytesMiddleware(limit int64) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, limit)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// corsMiddleware applies a strict CORS policy: only explicitly allow-listed
// origins are echoed back, and credentials are only enabled for a specific
// origin (never combined with "*"). With no configured origins, no CORS headers
// are emitted (same-origin only).
func corsMiddleware(allowedOrigins []string) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" && slices.Contains(allowedOrigins, origin) {
				h := w.Header()
				h.Set("Access-Control-Allow-Origin", origin)
				h.Add("Vary", "Origin")
				h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Hub-Signature-256, Webhook-Timestamp, X-Idempotency-Key, X-Request-ID")
				h.Set("Access-Control-Allow-Credentials", "true")
				h.Set("Access-Control-Max-Age", "600")
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// adminAuthMiddleware gates the management route group with a bearer token. It
// FAILS CLOSED: if apiKey is unset every request gets 503, so the service can
// never be deployed accidentally wide-open. Comparison is constant-time.
func adminAuthMiddleware(apiKey string) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if apiKey == "" {
				writeError(w, http.StatusServiceUnavailable, "admin API key not configured")
				return
			}
			const prefix = "Bearer "
			authz := r.Header.Get("Authorization")
			if !strings.HasPrefix(authz, prefix) {
				w.Header().Set("WWW-Authenticate", "Bearer")
				writeError(w, http.StatusUnauthorized, "missing or malformed Authorization header")
				return
			}
			token := strings.TrimPrefix(authz, prefix)
			if !secureCompare(token, apiKey) {
				writeError(w, http.StatusUnauthorized, "invalid admin credentials")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// secureCompare compares two secrets in constant time. Both sides are hashed to
// a fixed length first so the comparison does not leak the secret's length.
func secureCompare(a, b string) bool {
	ah := sha256.Sum256([]byte(a))
	bh := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(ah[:], bh[:]) == 1
}
