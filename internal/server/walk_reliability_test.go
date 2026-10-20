package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/keypool"
)

func TestWalkPayloadValidation(t *testing.T) {
	for _, body := range []string{
		`null`, `[]`, `{}`, `{"choices":[]}`, `{"choices":[null]}`, `{"choices":[{}]}`,
		`{"choices":[{"message":{}}]}`, `{"choices":[{"message":{"content":42}}]}`,
		`{"choices":[{"message":{"content":null,"tool_calls":[{}]}}]}`,
	} {
		if validateChatCompletion([]byte(body)) == nil {
			t.Errorf("accepted invalid completion: %s", body)
		}
	}
	for _, body := range []string{
		`{"choices":[{"message":{"content":""},"finish_reason":"stop"}]}`,
		`{"choices":[{"message":{"content":null,"tool_calls":[{"id":"call_1","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
		`{"choices":[{"message":{"content":null,"refusal":"cannot comply"},"finish_reason":"stop"}]}`,
	} {
		if err := validateChatCompletion([]byte(body)); err != nil {
			t.Errorf("rejected completion %s: %v", body, err)
		}
	}
	for _, vector := range []string{`null`, `[]`, `42`, `{}`, `true`, `"not base64"`, `"AA=="`, `[1,null]`, `["1"]`, `"AACAfw=="`} {
		body := `{"data":[{"embedding":[1]},{"embedding":` + vector + `}]}`
		if validateEmbeddings([]byte(body)) == nil {
			t.Errorf("accepted invalid second embedding: %s", body)
		}
	}
	for _, vector := range []string{`[0,-1.5,2e3]`, `"AACAPwAAAEA="`} {
		if err := validateEmbeddings([]byte(`{"data":[{"embedding":` + vector + `}]}`)); err != nil {
			t.Errorf("rejected valid vector %s: %v", vector, err)
		}
	}
}

func walkRequest(route string, ctx context.Context) *http.Request {
	r := httptest.NewRequest("POST", route, strings.NewReader(`{"model":"main","input":"hi","messages":[{"role":"user","content":"hi"}]}`)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer client-1")
	return r
}

func TestWalkBodyFaultsFallBack(t *testing.T) {
	oldBudget, oldMax := chatStepBudget, maxResponseBytes
	chatStepBudget, maxResponseBytes = 100*time.Millisecond, 512
	t.Cleanup(func() { chatStepBudget, maxResponseBytes = oldBudget, oldMax })
	t.Setenv("GUVNA_AUTO_STEP_BUDGET", "100ms")
	for _, tc := range []struct {
		name, route string
		aggregate   bool
	}{
		{"chat", "/v1/chat/completions", false}, {"auto", "/v1/auto", false}, {"embed", "/v1/embeddings", false}, {"responses", "/v1/responses", false},
		{"aggregated_chat", "/v1/chat/completions", true}, {"aggregated_auto", "/v1/auto", true}, {"aggregated_responses", "/v1/responses", true},
	} {
		for _, fault := range []string{"stalled", "oversized", "invalid"} {
			t.Run(tc.name+"_"+fault, func(t *testing.T) {
				var badHits, goodHits atomic.Int32
				bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					badHits.Add(1)
					w.WriteHeader(200)
					if fault == "stalled" {
						w.(http.Flusher).Flush()
						<-r.Context().Done()
					} else if fault == "oversized" {
						io.WriteString(w, strings.Repeat("x", 1024))
					} else if tc.route == "/v1/embeddings" {
						io.WriteString(w, `{"data":[{"embedding":[1]},{"embedding":42}]}`)
					} else {
						io.WriteString(w, `{"choices":[null]}`)
					}
				}))
				defer bad.Close()
				good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					goodHits.Add(1)
					if tc.route == "/v1/embeddings" {
						io.WriteString(w, `{"data":[{"embedding":[1,2]}]}`)
						return
					}
					io.WriteString(w, `{"choices":[{"message":{"content":"backup"},"finish_reason":"stop"}]}`)
				}))
				defer good.Close()
				s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A", StreamOnly: tc.aggregate}, {Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "a"}, {Provider: "b", Model: "b"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
				w := httptest.NewRecorder()
				start := time.Now()
				s.Handler().ServeHTTP(w, walkRequest(tc.route, context.Background()))
				if w.Code != 200 || badHits.Load() != 1 || goodHits.Load() != 1 || time.Since(start) > 2*time.Second {
					t.Fatalf("fault did not fall back promptly: status=%d hits=%d/%d body=%s", w.Code, badHits.Load(), goodHits.Load(), w.Body)
				}
				for _, st := range s.hlth.Snapshot() {
					if st.Provider == "a" && st.Failures != 1 {
						t.Fatalf("bad health=%v", st)
					}
				}
				if s.pools["a"].Keys()[0].Failures != 1 {
					t.Fatal("bad body was not charged to its key exactly once")
				}
			})
		}
	}
}

func TestWalkBodyCancellationStopsWithoutPenalty(t *testing.T) {
	for _, tc := range []struct {
		name, route string
		aggregate   bool
	}{
		{"chat", "/v1/chat/completions", false}, {"auto", "/v1/auto", false}, {"embed", "/v1/embeddings", false}, {"responses", "/v1/responses", false},
		{"aggregated_chat", "/v1/chat/completions", true}, {"aggregated_auto", "/v1/auto", true}, {"aggregated_responses", "/v1/responses", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			started := make(chan struct{})
			var badHits, backupHits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				badHits.Add(1)
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				close(started)
				<-r.Context().Done()
			}))
			defer up.Close()
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { backupHits.Add(1) }))
			defer backup.Close()
			s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A", StreamOnly: tc.aggregate}, {Name: "b", Type: "openai", BaseURL: backup.URL, KeyEnv: "B"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "a"}, {Provider: "b", Model: "b"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
			s.hlth.Mark("a", "a", 500, nil)
			seedRecoveredKey(t, s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			w := httptest.NewRecorder()
			go func() { defer close(done); s.Handler().ServeHTTP(w, walkRequest(tc.route, ctx)) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("upstream did not start")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("cancel did not stop body read")
			}
			if badHits.Load() != 1 || backupHits.Load() != 0 || w.Body.Len() != 0 {
				t.Fatalf("cancel retried or wrote body: hits=%d/%d body=%s", badHits.Load(), backupHits.Load(), w.Body)
			}
			failures := -1
			for _, st := range s.hlth.Snapshot() {
				if st.Provider == "a" {
					failures = st.Failures
				}
			}
			if failures != 1 || s.pools["a"].Keys()[0].Failures != 1 {
				t.Fatalf("cancel penalized health/key: %v/%v", s.hlth.Snapshot(), s.pools["a"].Keys())
			}
			runtimeTelemetry(t, s, 0, 0, false)
		})
	}
}

func TestWalkChatStreamIdleDeadline(t *testing.T) {
	oldIdle := streamIdle
	streamIdle = 80 * time.Millisecond
	t.Cleanup(func() { streamIdle = oldIdle })
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	s := runtimeServer(t, up.URL)
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"main","stream":true}`))
	r.Header.Set("Authorization", "Bearer client-1")
	w := httptest.NewRecorder()
	start := time.Now()
	s.Handler().ServeHTTP(w, r)
	if time.Since(start) > time.Second || !strings.Contains(w.Body.String(), "upstream stream did not complete") {
		t.Fatalf("idle deadline did not fire: %s", w.Body)
	}
	if s.hlth.Snapshot()[0].Failures != 1 || s.pools["a"].Keys()[0].Failures != 1 {
		t.Fatal("idle timeout was not charged exactly once")
	}
}

func TestWalkDownstreamWriteFailurePreservesHealth(t *testing.T) {
	for _, route := range []string{"/v1/chat/completions", "/v1/auto", "/v1/embeddings", "/v1/responses"} {
		t.Run(route, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if route == "/v1/embeddings" {
					io.WriteString(w, `{"data":[{"embedding":[1]}]}`)
					return
				}
				io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			s.hlth.Mark("a", "m", 500, nil)
			seedRecoveredKey(t, s)
			w := &runtimeFailWriter{ResponseRecorder: httptest.NewRecorder()}
			s.Handler().ServeHTTP(w, walkRequest(route, context.Background()))
			if s.hlth.Snapshot()[0].Failures != 1 || s.pools["a"].Keys()[0].Failures != 1 {
				t.Fatal("downstream failure reset or penalized upstream health")
			}
			runtimeTelemetry(t, s, 0, 0, false)
		})
	}
}

func TestWalkCancellationDuringBackoffDoesNotRetry(t *testing.T) {
	for _, route := range []string{"/v1/chat/completions", "/v1/auto", "/v1/embeddings"} {
		t.Run(route, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(503) }))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			s.hlth.Mark("a", "m", 500, nil)
			// Backoff only occurs if another key is available after the first
			// 503 quarantines A. With one key, the step now fails over at once.
			pool, err := keypool.New("a", []string{"A", "A2"}, "round_robin", config.QuarantineCfg{})
			if err != nil {
				t.Fatal(err)
			}
			s.pools["a"] = pool
			s.env = func(string) string { return "dummy" }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.retryWait = func(int) time.Duration { cancel(); return time.Second }
			start := time.Now()
			s.Handler().ServeHTTP(httptest.NewRecorder(), walkRequest(route, ctx))
			if hits.Load() != 1 || time.Since(start) > time.Second {
				t.Fatal("cancellation did not interrupt retry backoff")
			}
			if s.hlth.Snapshot()[0].Failures != 1 || s.pools["a"].Keys()[0].Failures != 1 {
				t.Fatal("cancel added a failure beyond the first upstream 503")
			}
			runtimeTelemetry(t, s, 0, 0, false)
		})
	}
}

func TestWalkStreamingErrorBodyHasDeadline(t *testing.T) {
	oldIdle, oldRetries := streamIdle, retries
	streamIdle, retries = 80*time.Millisecond, 0
	t.Cleanup(func() { streamIdle, retries = oldIdle, oldRetries })
	for _, route := range []string{"/v1/chat/completions", "/v1/auto", "/v1/responses"} {
		t.Run(route, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(503)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			r := httptest.NewRequest("POST", route, strings.NewReader(`{"model":"main","input":"hi","stream":true}`))
			r.Header.Set("Authorization", "Bearer client-1")
			start := time.Now()
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != 503 || time.Since(start) > time.Second {
				t.Fatalf("streaming error body did not expire: status=%d", w.Code)
			}
		})
	}
}
