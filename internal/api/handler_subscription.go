package api

import (
	"net/http"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/idgen"
)

// handleCreateSubscription validates the target URL, mints a signing secret, and
// persists the subscription. The plaintext secret is returned exactly once.
func (s *Server) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req createSubscriptionRequest
	if !readJSON(w, r, &req) {
		return
	}
	if err := domain.ValidateTargetURL(req.TargetURL, s.opts.AllowHTTPURLs); err != nil {
		writeDomainError(w, r, err)
		return
	}
	secret, err := idgen.NewSecretKey()
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	sub := &domain.Subscription{
		TargetURL:  req.TargetURL,
		SecretKey:  secret,
		EventTypes: req.EventTypes,
		IsActive:   req.IsActive == nil || *req.IsActive, // default true
	}
	if err := s.opts.Subscriptions.Create(r.Context(), sub); err != nil {
		writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, createSubscriptionResponse{
		subscriptionResponse: toSubscriptionResponse(sub),
		SecretKey:            secret,
	})
}

// handleListSubscriptions returns one keyset page: {items, next_cursor}.
func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 20, 100)
	cursor, err := decodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid cursor")
		return
	}
	page, err := s.opts.Subscriptions.List(r.Context(), limit, cursor)
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	items := make([]subscriptionResponse, 0, len(page.Items))
	for _, sub := range page.Items {
		items = append(items, toSubscriptionResponse(sub))
	}
	writeJSON(w, http.StatusOK, listSubscriptionsResponse{
		Items:      items,
		NextCursor: encodeCursor(page.NextCursor),
	})
}

// handleGetSubscription returns a single subscription (secret-redacted).
func (s *Server) handleGetSubscription(w http.ResponseWriter, r *http.Request) {
	sub, err := s.opts.Subscriptions.GetByID(r.Context(), pathID(r))
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toSubscriptionResponse(sub))
}

// handleUpdateSubscription applies a partial update (target_url, event_types,
// is_active). Secret rotation is a separate endpoint (Slice 8).
func (s *Server) handleUpdateSubscription(w http.ResponseWriter, r *http.Request) {
	var req updateSubscriptionRequest
	if !readJSON(w, r, &req) {
		return
	}
	sub, err := s.opts.Subscriptions.GetByID(r.Context(), pathID(r))
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	if req.TargetURL != nil {
		if err := domain.ValidateTargetURL(*req.TargetURL, s.opts.AllowHTTPURLs); err != nil {
			writeDomainError(w, r, err)
			return
		}
		sub.TargetURL = *req.TargetURL
	}
	if req.EventTypes != nil {
		sub.EventTypes = *req.EventTypes
	}
	if req.IsActive != nil {
		sub.IsActive = *req.IsActive
	}
	if err := s.opts.Subscriptions.Update(r.Context(), sub); err != nil {
		writeDomainError(w, r, err)
		return
	}
	s.opts.EvictSubscription(sub.ID)
	writeJSON(w, http.StatusOK, toSubscriptionResponse(sub))
}

// handleRotateSecret generates a new signing secret (demoting the current to the
// previous within the grace window), evicts the cache, and returns the new
// plaintext exactly once.
func (s *Server) handleRotateSecret(w http.ResponseWriter, r *http.Request) {
	id := pathID(r)
	newSecret, err := idgen.NewSecretKey()
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	sub, err := s.opts.Subscriptions.RotateSecret(r.Context(), id, newSecret)
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	s.opts.EvictSubscription(id)
	writeJSON(w, http.StatusOK, createSubscriptionResponse{
		subscriptionResponse: toSubscriptionResponse(sub),
		SecretKey:            newSecret,
	})
}

// handleDeleteSubscription removes a subscription (cascading its delivery_logs)
// and evicts its in-memory per-sub state (rate bucket, semaphore, cache).
func (s *Server) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	id := pathID(r)
	if err := s.opts.Subscriptions.Delete(r.Context(), id); err != nil {
		writeDomainError(w, r, err)
		return
	}
	s.opts.EvictSubscription(id)
	w.WriteHeader(http.StatusNoContent)
}

// handleListAttempts returns recent delivery attempts for a subscription.
func (s *Server) handleListAttempts(w http.ResponseWriter, r *http.Request) {
	id := pathID(r)
	if _, err := s.opts.Subscriptions.GetByID(r.Context(), id); err != nil {
		writeDomainError(w, r, err)
		return
	}
	limit := parseLimit(r, 50, 200)
	logs, err := s.opts.DeliveryLogs.ListBySubscription(r.Context(), id, limit)
	if err != nil {
		writeDomainError(w, r, err)
		return
	}
	items := make([]attemptResponse, 0, len(logs))
	for _, d := range logs {
		items = append(items, toAttemptResponse(d))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}
