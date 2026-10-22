package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestChatStreamChoiceCompleteness(t *testing.T) {
	for _, tc := range []struct {
		name   string
		n      int
		chunks []string
		eof    bool
		failed bool
	}{
		{"unfinished second", 2, []string{`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":{"content":"partial"}}]}`}, false, true},
		{"omitted second", 2, []string{`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`}, false, true},
		{"both complete", 2, []string{`{"choices":[{"index":1,"delta":{"content":"b"},"finish_reason":"length"},{"index":0,"delta":{"content":"a"},"finish_reason":"stop"}]}`, `{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3}}`}, false, false},
		{"both complete at EOF", 2, []string{`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":{},"finish_reason":"stop"}]}`}, true, false},
		{"unfinished at EOF", 2, []string{`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":{"content":"partial"}}]}`}, true, true},
		{"default observes unfinished extra", 0, []string{`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":{"content":"partial"}}]}`}, false, true},
		{"single requested preserves extra", 1, []string{`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":{},"finish_reason":"stop"}]}`}, false, false},
		{"negative index", 2, []string{`{"choices":[{"index":-1,"delta":{},"finish_reason":"stop"}]}`}, false, true},
		{"duplicate index", 2, []string{`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":0,"delta":{},"finish_reason":"stop"}]}`}, false, true},
		{"out of range index", 2, []string{`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":2,"delta":{},"finish_reason":"stop"}]}`}, false, true},
		{"missing index", 2, []string{`{"choices":[{"delta":{},"finish_reason":"stop"}]}`}, false, true},
		{"conflicting finish", 0, []string{`{"choices":[{"delta":{},"finish_reason":"stop"}]}`, `{"choices":[{"delta":{},"finish_reason":"length"}]}`}, false, true},
		{"content after finish", 0, []string{`{"choices":[{"delta":{},"finish_reason":"stop"}]}`, `{"choices":[{"delta":{"content":"late"}}]}`}, false, true},
		{"tool data after finish", 0, []string{`{"choices":[{"delta":{},"finish_reason":"stop"}]}`, `{"choices":[{"delta":{"tool_calls":[{"index":0}]}}]}`}, false, true},
		{"empty repeat finish", 0, []string{`{"choices":[{"delta":{},"finish_reason":"stop"}]}`, `{"choices":[{"delta":{"content":"","tool_calls":[]},"finish_reason":"stop"}]}`}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := sseBody(tc.chunks...)
			if tc.eof {
				wire = "data: " + strings.Join(tc.chunks, "\n\ndata: ")
			}
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, wire)
			}))
			defer up.Close()
			s := runtimeServer(t, up.URL)
			s.hlth.Mark("a", "m", 500, nil)
			seedRecoveredKey(t, s)
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"main","stream":true,"n":`+strconv.Itoa(tc.n)+`,"messages":[{"role":"user","content":"hi"}]}`))
			r.Header.Set("Authorization", "Bearer client-1")
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			body := w.Body.String()
			if w.Code != http.StatusOK || strings.Contains(body, `"error"`) != tc.failed || tc.failed && strings.Contains(body, "data: [DONE]") {
				t.Fatalf("incorrect stream terminal: status=%d body=%s", w.Code, body)
			}
			wantFailures, wantKeyFailures, wantStatus := 0, 0, http.StatusOK
			if tc.failed {
				wantFailures, wantKeyFailures, wantStatus = 2, 1, http.StatusBadGateway
			}
			if s.hlth.Snapshot()[0].Failures != wantFailures || s.pools["a"].Keys()[0].Failures != wantKeyFailures {
				t.Fatalf("wrong health/key attribution: health=%v keys=%v", s.hlth.Snapshot(), s.pools["a"].Keys())
			}
			runtimeTelemetry(t, s, 1, wantStatus, tc.failed)
		})
	}
}

func TestStreamOnlyOmittedRequestedChoiceFallsBackBeforeCommit(t *testing.T) {
	for _, route := range []string{"chat", "auto"} {
		t.Run(route, func(t *testing.T) {
			testOmittedRequestedChoiceFallback(t, route)
		})
	}
}

func testOmittedRequestedChoiceFallback(t *testing.T, route string) {
	t.Helper()
	var badHits, goodHits atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badHits.Add(1)
		io.WriteString(w, sseBody(`{"choices":[{"index":0,"delta":{"content":"wrong"},"finish_reason":"stop"}]}`))
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		io.WriteString(w, `{"choices":[{"index":0,"message":{"content":"backup-a"},"finish_reason":"stop"},{"index":1,"message":{"content":"backup-b"},"finish_reason":"stop"}]}`)
	}))
	defer good.Close()
	providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A", StreamOnly: true}, {Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B"}}
	chains := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "a"}, {Provider: "b", Model: "b"}}}}
	path, model := "/v1/chat/completions", "main"
	if route == "auto" {
		path, model = "/v1/auto", "auto"
		chains = []config.Chain{{Name: "fam-a", Steps: []config.Step{{Provider: "a", Model: "a"}}}, {Name: "fam-b", Steps: []config.Step{{Provider: "b", Model: "b"}}}}
	}
	s, _ := testServerDefault(t, providers, chains, map[string]string{"A": "dummy", "B": "dummy"}, "")
	r := httptest.NewRequest("POST", path, strings.NewReader(`{"model":"`+model+`","n":2,"messages":[{"role":"user","content":"hi"}]}`))
	r.Header.Set("Authorization", "Bearer client-1")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "backup-a") || !strings.Contains(w.Body.String(), "backup-b") || strings.Contains(w.Body.String(), "wrong") || badHits.Load() != 1 || goodHits.Load() != 1 {
		t.Fatalf("missing choice was committed: status=%d hits=%d/%d body=%s", w.Code, badHits.Load(), goodHits.Load(), w.Body)
	}
	for _, st := range s.hlth.Snapshot() {
		if st.Provider == "a" && st.Failures != 1 {
			t.Fatalf("missing choice failures=%d, want 1", st.Failures)
		}
	}
}

func TestChatStreamMultipleChoicesRemainStrictWithTerminatorOptOut(t *testing.T) {
	old := requireStreamTerminator
	requireStreamTerminator = false
	t.Cleanup(func() { requireStreamTerminator = old })
	for _, expected := range []int{0, 2} {
		progress := chatSSEProgress{expected: expected}
		_, err := progress.observe(`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":{"content":"partial"}}]}`)
		if err != nil {
			t.Fatal(err)
		}
		if err := progress.complete(requireStreamTerminator); err == nil {
			t.Fatal("single-choice compatibility setting hid an unfinished choice")
		}
	}
}
