package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/health"
	"github.com/creamy-ghost/guvna/internal/keypool"
)

func statusAttributionServer(t *testing.T, tc admissionRoute, status int) (*Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, `{"error":{"message":"upstream rejected request"}}`)
	}))
	t.Cleanup(up.Close)
	s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnvs: []string{"A1", "A2", "A3"}, StreamOnly: tc.aggregate}},
		[]config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}},
		map[string]string{"A1": "dummy1", "A2": "dummy2", "A3": "dummy3"})
	return s, &calls
}

func TestStatusAttributionAcrossRoutes(t *testing.T) {
	statuses := []struct {
		status   int
		keyClass keypool.Class
		failure  bool
	}{
		{400, "", false},
		{401, keypool.ClassAuth, false},
		{403, keypool.ClassAuth, false},
		{404, "", false},
		{408, "", false},
		{422, "", false},
		{429, keypool.ClassRateLimit, true},
		{500, "", true},
		{503, "", true},
	}
	for _, tc := range admissionRoutes {
		for _, st := range statuses {
			t.Run(fmt.Sprintf("%s/%d", tc.name, st.status), func(t *testing.T) {
				s, calls := statusAttributionServer(t, tc, st.status)
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, tc.request(context.Background()))
				if w.Code != st.status || !strings.Contains(w.Body.String(), "upstream rejected request") {
					t.Fatalf("upstream error changed: status=%d body=%s", w.Code, w.Body.String())
				}
				attempts := 1
				if st.keyClass != "" || st.status >= 500 {
					attempts = min(retries+1, 3)
				}
				if got := int(calls.Load()); got != attempts {
					t.Errorf("upstream calls=%d, want %d", got, attempts)
				}
				wantFailures := 0
				if st.failure {
					wantFailures = 1 // Count a failed step, not each key retry.
				}
				states := s.hlth.Snapshot()
				if len(states) != 1 || states[0].Failures != wantFailures {
					t.Fatalf("model health=%+v, want failures=%d", states, wantFailures)
				}
				if states[0].Down != (wantFailures >= health.FailureThreshold) {
					t.Errorf("unexpected cooldown: %+v", states[0])
				}
				for i, key := range s.pools["a"].Keys() {
					if st.keyClass != "" && i < attempts {
						if key.State != "quarantined" || key.Reason != string(st.keyClass) || key.Failures != 1 {
							t.Errorf("attempted key attributed incorrectly: %+v", key)
						}
					} else if key.State != "healthy" || key.Failures != 0 {
						t.Errorf("neutral or unattempted key penalized: %+v", key)
					}
				}
			})
		}
	}
}

func TestNeutralHTTPStatusReleasesHalfOpenAcrossRoutes(t *testing.T) {
	for _, tc := range admissionRoutes {
		for _, status := range []int{400, 401, 403, 404, 408, 422} {
			t.Run(fmt.Sprintf("%s/%d", tc.name, status), func(t *testing.T) {
				s, _ := statusAttributionServer(t, tc, status)
				seedHalfOpen(s)
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, tc.request(context.Background()))
				if w.Code != status {
					t.Fatalf("status=%d, want %d", w.Code, status)
				}
				requireProbeReleased(t, s)
				if s.hlth.IsDown("a", "m") {
					t.Fatal("neutral HTTP error renewed the model cooldown")
				}
			})
		}
	}
}
