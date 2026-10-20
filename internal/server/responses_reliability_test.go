package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/responses"
	"github.com/creamy-ghost/guvna/internal/router"
)

func TestResponsesStreamIdleDeadline(t *testing.T) {
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
	r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"main","input":"hi","stream":true}`))
	r.Header.Set("Authorization", "Bearer client-1")
	w := httptest.NewRecorder()
	start := time.Now()
	s.Handler().ServeHTTP(w, r)
	if time.Since(start) > time.Second || !strings.Contains(w.Body.String(), "event: response.failed\n") || strings.Contains(w.Body.String(), "event: response.completed\n") {
		t.Fatalf("idle deadline did not fail promptly: %s", w.Body)
	}
	if s.hlth.Snapshot()[0].Failures != 1 || s.pools["a"].Keys()[0].Failures != 1 {
		t.Fatal("idle timeout was not charged exactly once")
	}
}

type cancelAtEOFReader struct {
	io.Reader
	cancel context.CancelFunc
}

func (r cancelAtEOFReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		r.cancel()
	}
	return n, err
}

func TestResponsesCancellationAtBodyEOFDoesNotResetHealth(t *testing.T) {
	s := runtimeServer(t, "http://unused")
	s.hlth.Mark("a", "m", 500, nil)
	seedRecoveredKey(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := cancelAtEOFReader{strings.NewReader(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`), cancel}
	r := walkRequest("/v1/responses", ctx)
	w := httptest.NewRecorder()
	result := s.relayResponsesBody(w, r, router.Step{Provider: "a", Model: "m"}, "main", &responses.Request{Model: "main"}, &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(body)}, "A")
	if !result.clientGone || w.Body.Len() != 0 || s.hlth.Snapshot()[0].Failures != 1 || s.pools["a"].Keys()[0].Failures != 1 {
		t.Fatalf("canceled body altered state: result=%+v body=%s health=%v", result, w.Body, s.hlth.Snapshot())
	}
	runtimeTelemetry(t, s, 0, 0, false)
}

func responsesTestRequest(t *testing.T, ts *httptest.Server, stream bool) *http.Response {
	t.Helper()
	body := `{"model":"main","input":"hi","stream":false}`
	if stream {
		body = `{"model":"main","input":"hi","stream":true}`
	}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/responses", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer client-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestResponsesStreamRequiresFinishReasonAndPreservesLegalEOF(t *testing.T) {
	for _, tc := range []struct {
		name, tail, event string
		fail              bool
	}{
		{"DONE without finish", "data: [DONE]\n\n", "response.failed", true},
		{"EOF without finish", "", "response.failed", true},
		{"finish at EOF", `data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`, "response.completed", false},
		{"length at EOF", `data: {"choices":[{"delta":{},"finish_reason":"length"}]}`, "response.incomplete", false},
		{"empty tools", `data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`, "response.failed", true},
		{"incomplete tools", `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"f","arguments":"{"}}]},"finish_reason":"tool_calls"}]}`, "response.failed", true},
		{"tools at EOF", `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`, "response.completed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: "+`{"choices":[{"delta":{"content":"partial"}}]}`+"\n\n"+tc.tail)
			}))
			defer up.Close()
			s, ts := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "KEY_A"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"KEY_A": "k"})
			s.hlth.Mark("a", "m", 0, errors.New("prior failure"))
			resp := responsesTestRequest(t, ts, true)
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if !strings.Contains(string(body), "event: "+tc.event+"\n") {
				t.Fatalf("missing %s: %s", tc.event, body)
			}
			if tc.fail && (strings.Contains(string(body), "event: response.completed\n") || strings.Contains(string(body), "event: response.output_text.done\n")) {
				t.Fatalf("partial stream reported completion: %s", body)
			}
			for _, st := range s.hlth.Snapshot() {
				if st.Provider == "a" {
					want := 0
					if tc.fail {
						want = 2
					}
					if st.Failures != want {
						t.Fatalf("failures = %d, want %d", st.Failures, want)
					}
				}
			}
		})
	}
}

func TestStreamOnlyInvalidPayloadFallsBackBeforeCommit(t *testing.T) {
	for _, endpoint := range []string{"chat", "responses"} {
		t.Run(endpoint, func(t *testing.T) {
			bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, sseBody(`{"error":{"message":"broken"}}`, `{"choices":[{"delta":{"content":"wrong"},"finish_reason":"stop"}]}`))
			}))
			defer bad.Close()
			good := streamingOnlyUpstream(t, "fallback-answer")
			defer good.Close()
			providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "KEY_A", StreamOnly: true}, {Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "KEY_B", StreamOnly: true}}
			chains := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m-a"}, {Provider: "b", Model: "m-b"}}}}
			s, ts := testServer(t, providers, chains, map[string]string{"KEY_A": "k", "KEY_B": "k"})
			var resp *http.Response
			if endpoint == "chat" {
				resp = doChat(t, ts, "main", false)
			} else {
				resp = responsesTestRequest(t, ts, false)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != 200 || !strings.Contains(string(body), "fallback-answer") || strings.Contains(string(body), "wrong") {
				t.Fatalf("fallback failed: %d %s", resp.StatusCode, body)
			}
			for _, st := range s.hlth.Snapshot() {
				if st.Provider == "a" && st.Failures != 1 {
					t.Fatalf("invalid aggregate failures = %d", st.Failures)
				}
			}
		})
	}
}

func TestResponsesInvalidSuccessWithoutFallbackReturns502(t *testing.T) {
	for _, streamOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "body", true: "aggregate"}[streamOnly], func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if streamOnly {
					io.WriteString(w, sseBody(`{"error":{"message":"broken"}}`))
				} else {
					io.WriteString(w, `{"error":{"message":"broken"},"choices":[{}]}`)
				}
			}))
			defer up.Close()
			_, ts := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "KEY_A", StreamOnly: streamOnly}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "m"}}}}, map[string]string{"KEY_A": "k"})
			resp := responsesTestRequest(t, ts, false)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadGateway {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("invalid 2xx returned %d: %s", resp.StatusCode, body)
			}
		})
	}
}
