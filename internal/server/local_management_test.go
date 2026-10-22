package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/telemetry"
)

func TestPublicHandlerHasNoManagementRoutes(t *testing.T) {
	s := chainManagementServer(t)
	for _, route := range []struct{ method, path string }{
		{"GET", "/admin/status"}, {"GET", "/admin/logs"}, {"GET", "/admin/auto/scoring"},
		{"GET", "/v1/chains"}, {"POST", "/v1/chains"}, {"DELETE", "/v1/chains/main"},
	} {
		for _, token := range []string{"", "client-1", "admin-secret"} {
			for _, peer := range []string{"127.0.0.1:1234", "192.0.2.1:1234"} {
				r := httptest.NewRequest(route.method, route.path, strings.NewReader(`{"name":"public","steps":[{"provider":"a","model":"m"}]}`))
				r.RemoteAddr = peer
				r.Header.Set("Authorization", "Bearer "+token)
				r.Header.Set("X-Forwarded-For", "127.0.0.1")
				r.Header.Set("Forwarded", "for=127.0.0.1")
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				if w.Code != http.StatusNotFound {
					t.Fatalf("public %s %s token=%q peer=%s: status=%d body=%s", route.method, route.path, token, peer, w.Code, w.Body)
				}
			}
		}
	}
	if len(s.rtr.RuntimeChains()) != 0 {
		t.Fatal("public management request changed runtime chains")
	}
	for _, path := range []string{"/healthz", "/v1/models"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer client-1")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("public API %s changed: %d", path, w.Code)
		}
	}
}

func TestLocalManagementRequiresAdminForEveryRoute(t *testing.T) {
	s := chainManagementServer(t)
	for _, route := range []struct{ method, path string }{
		{"GET", "/admin/status"}, {"GET", "/admin/logs"}, {"GET", "/admin/auto/scoring"},
		{"GET", "/v1/chains"}, {"POST", "/v1/chains"}, {"DELETE", "/v1/chains/main"},
	} {
		for _, token := range []string{"", "client-1"} {
			r := httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`))
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			s.AdminHandler().ServeHTTP(w, r)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("local %s %s accepted non-admin: %d", route.method, route.path, w.Code)
			}
		}
	}
	if len(s.rtr.RuntimeChains()) != 0 {
		t.Fatal("non-admin request changed runtime chains")
	}
}

func scoringRequest(s *Server, query string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", "/admin/auto/scoring"+query, nil)
	r.Header.Set("Authorization", "Bearer admin-secret")
	w := httptest.NewRecorder()
	s.AdminHandler().ServeHTTP(w, r)
	return w
}

func TestLocalScoringReportsPassiveCollectionAndCurrentHealth(t *testing.T) {
	s, _ := testServer(t, []config.Provider{
		{Name: "a", Type: "openai", BaseURL: "http://unused", KeyEnv: "A"},
		{Name: "b", Type: "openai", BaseURL: "http://unused", KeyEnv: "B"},
	}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
	now := time.Now()
	for i, event := range []telemetry.Event{
		{Status: 200}, {Status: 429, FailureClass: "rate_limit"},
		{Status: 401, FailureClass: "auth"}, {Status: 400, FailureClass: "model_incompatible"},
	} {
		event.Provider, event.Model, event.RequestID = "a", "m", s.newRequestID()
		event.Ts, event.Attempt, event.LatencyMS = now, 1, int64(10+i)
		s.tm.Record(event)
	}
	s.tm.RecordAutoRequest(telemetry.AutoRequestEvent{RequestID: "complete", StartedAt: now.Add(-500 * time.Millisecond), FinishedAt: now,
		Mode: "decider", Policy: "static", Attempts: 2, Candidates: 2, Success: true, Status: 200, LatencyMS: 500})
	for i := 0; i < health.FailureThreshold; i++ {
		s.hlth.Mark("b", "cold", 503, nil)
	}
	if err := s.tm.Flush(); err != nil {
		t.Fatal(err)
	}
	q := url.Values{"since": {now.Add(-time.Hour).Format(time.RFC3339Nano)}, "until": {now.Add(time.Second).Format(time.RFC3339Nano)}}
	w := scoringRequest(s, "?"+q.Encode())
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"mode", "min_samples", "metrics", "comparisons", "comparison_retention", "auto_requests", "provider_health", "current_model_health", "provider_overview", "observed_at", "telemetry"} {
		if len(raw[field]) == 0 || string(raw[field]) == "null" {
			t.Errorf("missing report field %s: %s", field, w.Body)
		}
	}
	var got struct {
		AutoRequests       telemetry.AutoRequestSnapshot    `json:"auto_requests"`
		ProviderHealth     telemetry.ProviderHealthSnapshot `json:"provider_health"`
		CurrentModelHealth []health.StepStatus              `json:"current_model_health"`
		ProviderOverview   []autoProviderOverview           `json:"provider_overview"`
		Telemetry          telemetry.FlushStats             `json:"telemetry"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.AutoRequests.Policies) != 1 || got.AutoRequests.Policies[0].Requests != 1 || got.AutoRequests.Policies[0].FallbackRequests != 1 {
		t.Fatalf("request report=%+v", got.AutoRequests)
	}
	if len(got.ProviderHealth.Providers) != 1 {
		t.Fatalf("provider report=%+v", got.ProviderHealth)
	}
	p := got.ProviderHealth.Providers[0]
	if p.Provider != "a" || p.Attempts != 4 || p.RateLimited != 1 || p.AuthFailures != 1 || p.RequestErrors != 1 || p.HealthSamples != 2 || p.ServiceSuccessRate == nil || *p.ServiceSuccessRate != 0.5 {
		t.Fatalf("provider attribution=%+v", p)
	}
	if len(got.ProviderOverview) != 2 || got.ProviderOverview[0].ObservationStatus != "observed" || got.ProviderOverview[1].ObservationStatus != "no_data" || got.ProviderOverview[1].DownModels != 1 {
		t.Fatalf("historical observations mixed with breaker state: %+v", got.ProviderOverview)
	}
	if len(got.CurrentModelHealth) != 1 || !got.CurrentModelHealth[0].Down {
		t.Fatalf("current breaker state=%+v", got.CurrentModelHealth)
	}
}

func TestLocalScoringNoDataWindowsAndStorageErrors(t *testing.T) {
	for _, query := range []string{"", "?since=invalid", "?since=2020-01-01T00:00:00Z", "?since=2026-01-01T00:00:00Z&until=2026-01-01T00:00:00Z"} {
		s := chainManagementServer(t)
		w := scoringRequest(s, query)
		if query != "" {
			if w.Code != http.StatusBadRequest {
				t.Fatalf("invalid window %s: status=%d", query, w.Code)
			}
			continue
		}
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"observation_status":"no_data"`) || !strings.Contains(w.Body.String(), `"policies":[]`) || !strings.Contains(w.Body.String(), `"providers":[]`) {
			t.Fatalf("empty report misrepresented evidence: status=%d body=%s", w.Code, w.Body)
		}
	}
	for _, table := range []string{"auto_requests", "requests"} {
		s := chainManagementServer(t)
		if _, err := s.tm.DB().Exec("DROP TABLE " + table); err != nil {
			t.Fatal(err)
		}
		w := scoringRequest(s, "")
		if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "unavailable") {
			t.Fatalf("storage failure hidden: status=%d body=%s", w.Code, w.Body)
		}
	}
}
