package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestEmbeddingRoutesRejectUnusableResults(t *testing.T) {
	const good = `{"model":"backup","data":[{"index":1,"embedding":[3,4]},{"index":0,"embedding":[1,2]}]}`
	for _, tc := range []struct {
		name, bad string
	}{
		{"missing vector", `{"data":[{"index":0,"embedding":[1,2]}]}`},
		{"extra vector", `{"data":[{"index":0,"embedding":[1,2]},{"index":1,"embedding":[1,2]},{"index":2,"embedding":[1,2]}]}`},
		{"duplicate index", `{"data":[{"index":0,"embedding":[1,2]},{"index":0,"embedding":[3,4]}]}`},
		{"missing index", `{"data":[{"embedding":[1,2]},{"index":1,"embedding":[3,4]}]}`},
		{"null index", `{"data":[{"index":null,"embedding":[1,2]},{"index":1,"embedding":[3,4]}]}`},
		{"negative index", `{"data":[{"index":-1,"embedding":[1,2]},{"index":1,"embedding":[3,4]}]}`},
		{"out of range index", `{"data":[{"index":0,"embedding":[1,2]},{"index":2,"embedding":[3,4]}]}`},
		{"noninteger index", `{"data":[{"index":0.5,"embedding":[1,2]},{"index":1,"embedding":[3,4]}]}`},
		{"inconsistent dimensions", `{"data":[{"index":0,"embedding":[1,2]},{"index":1,"embedding":[3]}]}`},
		{"wrong requested dimensions", `{"data":[{"index":0,"embedding":[1]},{"index":1,"embedding":[3]}]}`},
		{"mixed encodings", `{"data":[{"index":0,"embedding":[1,2]},{"index":1,"embedding":"AACAPwAAAEA="}]}`},
		{"nonfinite base64", `{"data":[{"index":0,"embedding":"AACAPwAAAEA="},{"index":1,"embedding":"AACAfwAAAEA="}]}`},
		{"inconsistent base64 dimensions", `{"data":[{"index":0,"embedding":"AACAPwAAAEA="},{"index":1,"embedding":"AACAPw=="}]}`},
	} {
		for _, fallback := range []bool{false, true} {
			mode := "exhausted"
			if fallback {
				mode = "fallback"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				var hitsA, hitsB atomic.Int32
				bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					hitsA.Add(1)
					w.Header().Set("Content-Type", "application/json")
					io.WriteString(w, tc.bad)
				}))
				defer bad.Close()
				backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					hitsB.Add(1)
					w.Header().Set("Content-Type", "application/json")
					io.WriteString(w, good)
				}))
				defer backup.Close()
				steps := []config.Step{{Provider: "a", Model: "m-a"}}
				if fallback {
					steps = append(steps, config.Step{Provider: "b", Model: "m-b"})
				}
				s, _ := testServer(t, []config.Provider{
					{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A"},
					{Name: "b", Type: "openai", BaseURL: backup.URL, KeyEnv: "B"},
				}, []config.Chain{{Name: "main", Steps: steps}}, map[string]string{"A": "dummy", "B": "dummy"})
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, embeddingRouteRequest(context.Background(), `{"model":"main","input":["one","two"],"dimensions":2}`))
				wantStatus, wantB, wantRows := http.StatusBadGateway, int32(0), 1
				if fallback {
					wantStatus, wantB, wantRows = http.StatusOK, 1, 2
				}
				if w.Code != wantStatus || hitsA.Load() != 1 || hitsB.Load() != wantB {
					t.Fatalf("status=%d hits=%d/%d body=%s", w.Code, hitsA.Load(), hitsB.Load(), w.Body)
				}
				if fallback && w.Body.String() != good {
					t.Fatalf("invalid first body leaked or backup changed: %s", w.Body)
				}
				if !fallback && (strings.Contains(w.Body.String(), "embedding") || !strings.Contains(w.Body.String(), "unusable response")) {
					t.Fatalf("exhaustion did not return a safe error: %s", w.Body)
				}
				embeddingRouteFailures(t, s, "a", 1)
				embeddingRouteFailures(t, s, "b", 0)
				runtimeTelemetry(t, s, wantRows, wantStatus, !fallback)
			})
		}
	}
}

func TestEmbeddingRoutesEffectiveStepParameters(t *testing.T) {
	const numeric2 = `{"data":[{"embedding":[1,2]}]}`
	const numeric3 = `{"data":[{"embedding":[1,2,3]}]}`
	const base642 = `{"data":[{"embedding":"AACAPwAAAEA="}]}`
	for _, tc := range []struct {
		name, request, responseA, responseB string
		paramsA, paramsB                    map[string]any
		wantA, wantB                        string
		fallback                            bool
	}{
		{"dimensions vary per step", `{"model":"main","input":"one"}`, numeric2, numeric2,
			map[string]any{"dimensions": 3}, map[string]any{"dimensions": 2}, `{"dimensions":3,"input":"one"}`, `{"dimensions":2,"input":"one"}`, true},
		{"explicit dimensions win", `{"model":"main","input":"one","dimensions":2}`, numeric2, numeric3,
			map[string]any{"dimensions": 3}, map[string]any{"dimensions": 4}, `{"dimensions":2,"input":"one"}`, "", false},
		{"explicit null dimensions win", `{"model":"main","input":"one","dimensions":null}`, numeric3, numeric2,
			map[string]any{"dimensions": 2}, nil, `{"dimensions":null,"input":"one"}`, "", false},
		{"format varies per step", `{"model":"main","input":"one"}`, numeric2, base642,
			map[string]any{"encoding_format": "base64"}, map[string]any{"encoding_format": "base64"}, `{"encoding_format":"base64","input":"one"}`, `{"encoding_format":"base64","input":"one"}`, true},
		{"explicit format wins", `{"model":"main","input":"one","encoding_format":"float"}`, numeric2, base642,
			map[string]any{"encoding_format": "base64"}, nil, `{"encoding_format":"float","input":"one"}`, "", false},
		{"explicit null format wins", `{"model":"main","input":"one","encoding_format":null}`, base642, numeric2,
			map[string]any{"encoding_format": "float"}, nil, `{"encoding_format":null,"input":"one"}`, "", false},
		{"input varies per step", `{"model":"main"}`, numeric2, `{"data":[{"index":0,"embedding":[1,2]},{"index":1,"embedding":[3,4]},{"index":2,"embedding":[5,6]}]}`,
			map[string]any{"input": []string{"a", "b"}}, map[string]any{"input": []string{"a", "b", "c"}}, `{"input":["a","b"]}`, `{"input":["a","b","c"]}`, true},
		{"explicit input wins", `{"model":"main","input":"client"}`, numeric2, numeric3,
			map[string]any{"input": []string{"a", "b"}}, nil, `{"input":"client"}`, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hitsA, hitsB atomic.Int32
			seenA, seenB := make(chan map[string]json.RawMessage, 1), make(chan map[string]json.RawMessage, 1)
			upstream := func(response string, hits *atomic.Int32, seen chan<- map[string]json.RawMessage) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					hits.Add(1)
					var payload map[string]json.RawMessage
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Errorf("decode forwarded payload: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					seen <- payload
					io.WriteString(w, response)
				}))
			}
			a, b := upstream(tc.responseA, &hitsA, seenA), upstream(tc.responseB, &hitsB, seenB)
			defer a.Close()
			defer b.Close()
			s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: a.URL, KeyEnv: "A"}, {Name: "b", Type: "openai", BaseURL: b.URL, KeyEnv: "B"}},
				[]config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m-a", Params: tc.paramsA}, {Provider: "b", Model: "m-b", Params: tc.paramsB}}}}, map[string]string{"A": "dummy", "B": "dummy"})
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, embeddingRouteRequest(context.Background(), tc.request))
			wantB, wantBody := int32(0), tc.responseA
			if tc.fallback {
				wantB, wantBody = 1, tc.responseB
			}
			if w.Code != http.StatusOK || w.Body.String() != wantBody || hitsA.Load() != 1 || hitsB.Load() != wantB {
				t.Fatalf("status=%d hits=%d/%d body=%s", w.Code, hitsA.Load(), hitsB.Load(), w.Body)
			}
			embeddingRoutePayload(t, <-seenA, tc.wantA, "m-a")
			if tc.fallback {
				embeddingRoutePayload(t, <-seenB, tc.wantB, "m-b")
				embeddingRouteFailures(t, s, "a", 1)
			} else {
				embeddingRouteFailures(t, s, "a", 0)
			}
			embeddingRouteFailures(t, s, "b", 0)
		})
	}
}

func TestEmbeddingRoutesInputForms(t *testing.T) {
	for _, tc := range []struct {
		name, input, response string
	}{
		{"text", `"one"`, `{"data":[{"embedding":[1,2]}]}`},
		{"tokens", `[1,2,3]`, `{"data":[{"embedding":[1,2]}]}`},
		{"texts", `["one","two"]`, `{"data":[{"index":1,"embedding":[3,4]},{"index":0,"embedding":[1,2]}]}`},
		{"token batches", `[[1,2],[3]]`, `{"data":[{"index":0,"embedding":"AACAPwAAAEA="},{"index":1,"embedding":"AABAQAAAgEA="}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				io.WriteString(w, tc.response)
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, embeddingRouteRequest(context.Background(), `{"model":"main","input":`+tc.input+`,"dimensions":2}`))
			if w.Code != http.StatusOK || hits.Load() != 1 || w.Body.String() != tc.response {
				t.Fatalf("status=%d hits=%d body=%s", w.Code, hits.Load(), w.Body)
			}
			embeddingRouteFailures(t, s, "a", 0)
			runtimeTelemetry(t, s, 1, http.StatusOK, false)
		})
	}
}

func TestEmbeddingRoutesCancellationPreservesPriorFailures(t *testing.T) {
	started := make(chan struct{})
	var hitsA, hitsB atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsA.Add(1)
		io.WriteString(w, `{"data":[{"index":0,"embedding":[1,2]}`)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer up.Close()
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hitsB.Add(1) }))
	defer backup.Close()
	s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A"}, {Name: "b", Type: "openai", BaseURL: backup.URL, KeyEnv: "B"}},
		[]config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m-a"}, {Provider: "b", Model: "m-b"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
	s.hlth.Mark("a", "m-a", http.StatusInternalServerError, nil)
	seedRecoveredKey(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Handler().ServeHTTP(w, embeddingRouteRequest(ctx, `{"model":"main","input":["one","two"],"dimensions":2}`))
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("upstream did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not stop the incomplete body")
	}
	if hitsA.Load() != 1 || hitsB.Load() != 0 || w.Body.Len() != 0 {
		t.Fatalf("cancelled request wrote output or retried: hits=%d/%d body=%s", hitsA.Load(), hitsB.Load(), w.Body)
	}
	embeddingRouteFailures(t, s, "a", 1)
	embeddingRouteFailures(t, s, "b", 0)
	runtimeTelemetry(t, s, 0, 0, false)
}

func embeddingRouteRequest(ctx context.Context, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/embeddings", strings.NewReader(body)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer client-1")
	return r
}

func embeddingRouteFailures(t *testing.T, s *Server, provider string, want int) {
	t.Helper()
	got := 0
	for _, state := range s.hlth.Snapshot() {
		if state.Provider == provider {
			got += state.Failures
		}
	}
	if got != want || s.pools[provider].Keys()[0].Failures != want {
		t.Fatalf("%s health/key failures=%d/%d want=%d", provider, got, s.pools[provider].Keys()[0].Failures, want)
	}
}

func embeddingRoutePayload(t *testing.T, got map[string]json.RawMessage, wantFields, model string) {
	t.Helper()
	var want map[string]json.RawMessage
	if err := json.Unmarshal([]byte(wantFields), &want); err != nil {
		t.Fatal(err)
	}
	encodedModel, _ := json.Marshal(model)
	want["model"] = encodedModel
	for key, value := range want {
		if string(got[key]) != string(value) {
			t.Fatalf("forwarded %q=%s want=%s", key, got[key], value)
		}
	}
}
