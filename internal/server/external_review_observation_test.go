package server

// Regression coverage for the defects independently reproduced during review.
import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestExternalReviewObservations(t *testing.T) {
	for _, tc := range []struct {
		name         string
		payload      string
		stream       bool
		truncated    bool
		wantStatus   int
		wantFailures int
	}{
		{"unfinished_chat_sse", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n", true, false, 200, 3},
		{"invalid_chat_sse", "data: {broken-json}\n\n", true, false, 200, 3},
		{"done_without_finish", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\ndata: [DONE]\n\n", true, false, 200, 3},
		{"invalid_json_2xx", "{broken-json}", false, false, 200, 3},
		{"empty_2xx", "", false, false, 200, 3},
		{"truncated_body_falls_back", "{\"choices\":", false, true, 200, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.stream {
					w.Header().Set("Content-Type", "text/event-stream")
				} else {
					w.Header().Set("Content-Type", "application/json")
				}
				if tc.truncated {
					w.Header().Set("Content-Length", "1000")
				}
				w.WriteHeader(200)
				io.WriteString(w, tc.payload)
			}))
			defer bad.Close()
			var goodHits atomic.Int32
			good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				goodHits.Add(1)
				io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
			}))
			defer good.Close()
			s, ts := testServer(t, []config.Provider{
				{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A"},
				{Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B"},
			}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m-a"}, {Provider: "b", Model: "m-b"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
			s.hlth.Mark("a", "m-a", 500, nil)
			s.hlth.Mark("a", "m-a", 500, nil)
			resp := doChat(t, ts, "main", tc.stream)
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			failures := -1
			for _, st := range s.hlth.Snapshot() {
				if st.Provider == "a" {
					failures = st.Failures
				}
			}
			t.Logf("status=%d bad_failures=%d backup_hits=%d body=%q", resp.StatusCode, failures, goodHits.Load(), body)
			wantBackup := int32(1)
			if tc.stream {
				wantBackup = 0
			}
			if resp.StatusCode != tc.wantStatus || failures != tc.wantFailures || goodHits.Load() != wantBackup {
				t.Fatal("failure was not accounted for or fallback crossed the commit boundary")
			}
			if tc.stream && !strings.Contains(string(body), "upstream stream did not complete") {
				t.Fatal("committed stream omitted its failure event")
			}
			if !tc.stream && !strings.Contains(string(body), `"content":"ok"`) {
				t.Fatal("backup completion did not reach client")
			}
		})
	}
	t.Run("client_cancel_preserves_health_and_keys", func(t *testing.T) {
		up := completionUpstream(t, 200, `{"choices":[{}]}`, false)
		defer up.Close()
		s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m-a"}}}}, map[string]string{"A": "dummy"})
		s.hlth.Mark("a", "m-a", 500, nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"main"}`)).WithContext(ctx)
		w := httptest.NewRecorder()
		s.handleChat(w, r)
		ss, ks := s.hlth.Snapshot(), s.pools["a"].Keys()
		t.Logf("status=%d model_failures=%d key_failures=%d key_state=%s", w.Code, ss[0].Failures, ks[0].Failures, ks[0].State)
		if ss[0].Failures != 1 || ks[0].Failures != 0 || ks[0].State != "healthy" {
			t.Fatal("caller cancellation changed provider/key health")
		}
	})
}
