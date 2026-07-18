package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
)

// readJSON decodes the request body into dst, rejecting unknown fields. On any
// failure it writes the appropriate error response (413 for oversize bodies, 400
// otherwise) and returns false.
func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, "empty request body")
		return false
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	// Exactly one JSON value is allowed. Decoder.Decode by itself accepts a
	// second value (for example, `{} {}`), which is almost never intentional and
	// makes request interpretation ambiguous.
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

// writeDomainError maps a domain sentinel error to an HTTP status. Unexpected
// errors are logged with the request-scoped logger and surfaced as 500.
func writeDomainError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, domain.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, domain.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, domain.ErrInactiveSubscription):
		writeError(w, http.StatusForbidden, "subscription is inactive")
	default:
		loggerFrom(r.Context()).Error("unexpected handler error", "error", err, "path", r.URL.Path)
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

// pathID returns the {id} path value.
func pathID(r *http.Request) string { return r.PathValue("id") }

// parseClampedInt reads a named integer query param, returning def when it is
// absent or malformed (<1) and clamping to max. It is the single home for how the
// API treats bad / out-of-range query ints.
func parseClampedInt(r *http.Request, name string, def, max int) int {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// parseLimit reads the `limit` query param, clamped to [1, max].
func parseLimit(r *http.Request, def, max int) int { return parseClampedInt(r, "limit", def, max) }

// parseHours reads the `hours` query param, clamped to [1, 8760] (one year).
func parseHours(r *http.Request, def int) int { return parseClampedInt(r, "hours", def, 8760) }
