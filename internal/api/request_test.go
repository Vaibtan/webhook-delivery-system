package api

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Vaibtan/webhook-delivery-system/internal/domain"
	"github.com/Vaibtan/webhook-delivery-system/internal/infra/signature"
)

type stubSubscriptions struct{ sub *domain.Subscription }

func (s *stubSubscriptions) Create(context.Context, *domain.Subscription) error { return nil }
func (s *stubSubscriptions) GetByID(context.Context, string) (*domain.Subscription, error) {
	return s.sub, nil
}
func (s *stubSubscriptions) List(context.Context, int, *domain.Cursor) (domain.Page[*domain.Subscription], error) {
	return domain.Page[*domain.Subscription]{}, nil
}
func (s *stubSubscriptions) Update(context.Context, *domain.Subscription) error { return nil }
func (s *stubSubscriptions) Delete(context.Context, string) error               { return nil }
func (s *stubSubscriptions) RotateSecret(context.Context, string, string) (*domain.Subscription, error) {
	return s.sub, nil
}

type stubDeliveryIngest struct{ ingests int }

func (s *stubDeliveryIngest) GetByID(context.Context, string) (*domain.DeliveryLog, error) {
	return nil, domain.ErrNotFound
}
func (s *stubDeliveryIngest) IngestPending(context.Context, domain.IngestParams) (domain.IngestResult, error) {
	s.ingests++
	return domain.IngestResult{WebhookID: "123e4567-e89b-12d3-a456-426614174001", DeliveryLogID: "123e4567-e89b-12d3-a456-426614174002"}, nil
}

func TestReadJSONRejectsTrailingValue(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"first"} {"name":"second"}`))
	w := httptest.NewRecorder()
	var dst struct {
		Name string `json:"name"`
	}

	assert.False(t, readJSON(w, r, &dst))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestDecodeCursorRejectsNonUUIDID(t *testing.T) {
	raw := time.Now().UTC().Format(time.RFC3339Nano) + ",not-a-uuid"
	token := base64.RawURLEncoding.EncodeToString([]byte(raw))
	_, err := decodeCursor(token)
	require.Error(t, err)
}

func TestIngestDoesNotChargeRateLimitBeforeAuthentication(t *testing.T) {
	srv, logs, rateCalls := newIngestTestServer()
	body := []byte(`{"ok":true}`)
	r := signedIngestRequest(body, "bad-signature", "")
	w := httptest.NewRecorder()

	srv.handleIngest(w, r)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Equal(t, 0, *rateCalls)
	assert.Equal(t, 0, logs.ingests)
}

func TestIngestRejectsInvalidJSONAsBadRequest(t *testing.T) {
	srv, logs, _ := newIngestTestServer()
	body := []byte(`not-json`)
	r := signedIngestRequest(body, "", "")
	w := httptest.NewRecorder()

	srv.handleIngest(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, 0, logs.ingests)
}

func TestIngestRejectsOversizedEventTypeAsBadRequest(t *testing.T) {
	srv, logs, _ := newIngestTestServer()
	body := []byte(`{"ok":true}`)
	r := signedIngestRequest(body, "", strings.Repeat("x", domain.MaxEventTypeLength+1))
	w := httptest.NewRecorder()

	srv.handleIngest(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Equal(t, 0, logs.ingests)
}

func newIngestTestServer() (*Server, *stubDeliveryIngest, *int) {
	const subID = "123e4567-e89b-12d3-a456-426614174000"
	logs := &stubDeliveryIngest{}
	rateCalls := 0
	s := &Server{opts: Options{
		Subscriptions: &stubSubscriptions{sub: &domain.Subscription{
			ID: subID, TargetURL: "https://example.com/hook", SecretKey: "secret", IsActive: true,
		}},
		DeliveryIngest: logs,
		RateLimitAllow: func(string) bool {
			rateCalls++
			return true
		},
		SignatureDriftWindow: 5 * time.Minute,
	}}
	return s, logs, &rateCalls
}

func signedIngestRequest(body []byte, overrideSignature, eventType string) *http.Request {
	const subID = "123e4567-e89b-12d3-a456-426614174000"
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	sig := signature.Sign(signature.BuildMessage(ts, "", body), "secret")
	if overrideSignature != "" {
		sig = overrideSignature
	}
	path := "/api/v1/ingest/" + subID
	if eventType != "" {
		path += "?event_type=" + eventType
	}
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
	r.SetPathValue("id", subID)
	r.Header.Set("Webhook-Timestamp", ts)
	r.Header.Set("X-Hub-Signature-256", sig)
	return r
}
