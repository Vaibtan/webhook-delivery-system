package api

import "net/http"

// handleReplay atomically claims a DLQ entry and starts a fresh delivery chain
// (replay_number+1, attempt 1). Concurrent replays produce exactly one new chain
// (the loser gets 404). Admin auth.
func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	subID := pathID(r)
	webhookID := r.PathValue("webhook_id")

	claimedID, newID, ok, err := s.opts.DeliveryDLQ.ReplayDLQ(r.Context(), subID, webhookID)
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "no DLQ entry for that subscription and webhook")
		return
	}

	// Post-commit: enqueue the new chain and remove the old id from the DLQ LIST.
	if err := s.opts.TaskQueue.Enqueue(r.Context(), newID); err != nil {
		loggerFrom(r.Context()).Error("replay: enqueue failed", "delivery_id", newID, "error", err)
	}
	if err := s.opts.DLQ.Remove(r.Context(), claimedID); err != nil {
		loggerFrom(r.Context()).Warn("replay: dlq LREM failed", "id", claimedID, "error", err)
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"webhook_id":  webhookID,
		"delivery_id": newID,
		"status":      "replaying",
	})
}

// handleAckDLQ discards a DLQ entry without replaying. The atomic claim IS the
// ack. 404 if already reaped/acked. Admin auth.
func (s *Server) handleAckDLQ(w http.ResponseWriter, r *http.Request) {
	subID := pathID(r)
	webhookID := r.PathValue("webhook_id")

	claimedID, ok, err := s.opts.DeliveryDLQ.AckDLQ(r.Context(), subID, webhookID)
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "no DLQ entry for that subscription and webhook")
		return
	}
	if err := s.opts.DLQ.Remove(r.Context(), claimedID); err != nil {
		loggerFrom(r.Context()).Warn("ack: dlq LREM failed", "id", claimedID, "error", err)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"webhook_id": webhookID,
		"status":     "acknowledged",
	})
}
