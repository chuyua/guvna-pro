package server

import (
	"context"
	"github.com/creamy-ghost/guvna/internal/config"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCallerCancelStopsChainAndKeepsKeysHealthy(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/auto", "/v1/embeddings"} {
		t.Run(path, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				calls.Add(1)
				cancel()
				select {
				case <-r.Context().Done():
				case <-time.After(100 * time.Millisecond):
				}
			}))
			defer up.Close()
			providers := []config.Provider{{Name: "p", Type: "openai", BaseURL: up.URL, KeyEnvs: []string{"K1", "K2"}, Models: []string{"A", "B"}}}
			ch := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "p", Model: "A"}, {Provider: "p", Model: "B"}}}}
			s, _ := testServer(t, providers, ch, map[string]string{"K1": "k1", "K2": "k2"})
			r := httptest.NewRequest("POST", path, strings.NewReader(`{"model":"main","input":"hi","messages":[{"role":"user","content":"hi"}],"min_context":1}`)).WithContext(ctx)
			r.Header.Set("Authorization", "Bearer client-1")
			done := make(chan struct{})
			go func() { s.Handler().ServeHTTP(httptest.NewRecorder(), r); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("canceled request still running")
			}
			if calls.Load() != 1 {
				t.Fatalf("upstream calls=%d", calls.Load())
			}
			for _, row := range s.hlth.Snapshot() {
				if row.Failures != 0 {
					t.Fatalf("cancellation marked model down: %+v", row)
				}
			}
			for _, row := range s.pools["p"].Keys() {
				if row.Failures != 0 || row.State == "quarantined" {
					t.Fatalf("cancellation quarantined key: %+v", row)
				}
			}
		})
	}
}

func TestDeciderNemotronStructuredPayload(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		body := string(b)
		if !strings.Contains(body, `"enable_thinking":false`) || !strings.Contains(body, `"json_object"`) || !strings.Contains(body, `"max_tokens":8192`) {
			t.Errorf("unexpected payload: %s", body)
		}
		io.WriteString(w, `{"choices":[{"finish_reason":"stop","message":{"content":"{\"order\":[\"nvidia/A\",\"nvidia/B\"]}"}}]}`)
	}))
	defer up.Close()
	s, _ := testServer(t, []config.Provider{{Name: "nvidia", Type: "openai", BaseURL: up.URL, KeyEnv: "K"}}, nil, map[string]string{"K": "key"})
	s.decideProvider = "nvidia"
	s.decideModel = "nvidia/nemotron-3-ultra-550b-a55b"
	_, err := s.callDecider(nil, context.Background(), nil)
	if err == nil {
		t.Fatal("unknown ranked models should be rejected")
	}
}
