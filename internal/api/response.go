package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// writeJSON serializes v as JSON with the given status code. Encoding errors are
// logged (the header is already committed, so they cannot be surfaced to the client).
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("api: encode response", "error", err)
	}
}

// errorBody is the canonical error envelope for API responses.
type errorBody struct {
	Error string `json:"error"`
}

// writeError emits a JSON error envelope with the given status code.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg})
}
