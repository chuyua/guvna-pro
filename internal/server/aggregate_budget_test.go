package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

func aggregateTestLimit(t *testing.T, limit int64) {
	t.Helper()
	previous := maxResponseBytes
	maxResponseBytes = limit
	t.Cleanup(func() { maxResponseBytes = previous })
}

type aggregateReadTracker struct {
	r      io.Reader
	read   int
	closed bool
}

func (r *aggregateReadTracker) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.read += n
	return n, err
}

func (r *aggregateReadTracker) Close() error {
	r.closed = true
	return nil
}

func TestAggregateStreamedInputBoundary(t *testing.T) {
	const limit = 512
	chunk := `data: {"choices":[{"delta":{"content":"answer"},"finish_reason":"stop"}]}` + "\n\n"
	// A finished choice must not turn a reader-enforced truncation into success.
	// There is intentionally no [DONE], so aggregation must consume through EOF.
	exact := chunk + ":" + strings.Repeat("x", limit-len(chunk)-2) + "\n"
	for _, tc := range []struct {
		name, wire string
		oversized  bool
	}{
		{"exact", exact, false},
		{"one byte over", exact + "x", true},
		{"large unread tail", exact + strings.Repeat("x", 16*limit), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			aggregateTestLimit(t, limit)
			body := &aggregateReadTracker{r: strings.NewReader(tc.wire)}
			resp, err := (&Server{}).aggregateStreamed(&http.Response{Body: body})
			if tc.oversized {
				if err == nil || !strings.Contains(err.Error(), "input exceeds 512 bytes") || resp != nil {
					t.Fatalf("oversized input became a successful completion: resp=%v err=%v", resp, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
			}
			if !body.closed || body.read > limit+1 {
				t.Fatalf("input was not bounded and closed: read=%d closed=%v", body.read, body.closed)
			}
		})
	}
}

func TestAggregateStreamedRejectsSingleOversizedEvent(t *testing.T) {
	aggregateTestLimit(t, 512)
	wire := `data: {"choices":[{"delta":{"content":"` + strings.Repeat("x", 4096) + `"},"finish_reason":"stop"}]}`
	body := &aggregateReadTracker{r: strings.NewReader(wire)}
	resp, err := (&Server{}).aggregateStreamed(&http.Response{Body: body})
	if err == nil || resp != nil {
		t.Fatalf("accepted an oversized SSE event: resp=%v err=%v", resp, err)
	}
	if !body.closed || body.read > 513 {
		t.Fatalf("oversized event read without the configured budget: read=%d closed=%v", body.read, body.closed)
	}
}

func TestAggregateStreamedEncodedResponseBoundary(t *testing.T) {
	// Literal HTML and JSON line separators are compact upstream but expand when
	// the final JSON is encoded. Include interleaved choices and fragmented tools.
	text := strings.Repeat("<>&\u2028\u2029", 20)
	chunks := []string{
		`{"id":"old","model":"old","created":100,"choices":[{"index":1,"delta":{"tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"f","arguments":"{\"v\":\""}}]}},{"index":0,"delta":{"content":` + aggregateRawString(t, text) + `,"reasoning":"<"}}],"usage":{"notes":"<>&","total_tokens":1}}`,
		`{"id":"id","model":"m","created":9,"choices":[{"index":0,"delta":{"content":"\"\\\n\u0001","reasoning_content":"&"},"finish_reason":"stop"},{"index":1,"delta":{"tool_calls":[{"index":0,"function":{"arguments":` + aggregateRawString(t, text+`"}`) + `}}]},"finish_reason":"tool_calls"}],"usage": { "notes": ` + aggregateRawString(t, "<&\u2028") + `, "total_tokens": 2 }}`,
	}
	wire := sseBody(chunks...)
	aggregateTestLimit(t, 0)
	baseline, err := (&Server{}).aggregateStreamed(&http.Response{Body: io.NopCloser(strings.NewReader(wire))}, 2)
	if err != nil {
		t.Fatal(err)
	}
	want, err := io.ReadAll(baseline.Body)
	baseline.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(want) <= len(wire) {
		t.Fatalf("fixture must exercise output expansion: input=%d output=%d", len(wire), len(want))
	}
	for _, tc := range []struct {
		name  string
		limit int64
		fail  bool
	}{
		{"exact", int64(len(want)), false},
		{"one byte under", int64(len(want) - 1), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			aggregateTestLimit(t, tc.limit)
			resp, err := (&Server{}).aggregateStreamed(&http.Response{Body: io.NopCloser(strings.NewReader(wire))}, 2)
			if tc.fail {
				if err == nil || !strings.Contains(err.Error(), "aggregated upstream response exceeds") || resp != nil {
					t.Fatalf("unbounded accumulated output: resp=%v err=%v", resp, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("exact response boundary changed output: err=%v got=%s want=%s", err, got, want)
			}
		})
	}
}

func aggregateRawString(t *testing.T, s string) string {
	t.Helper()
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		t.Fatal(err)
	}
	// encoding/json always escapes U+2028/U+2029, while valid upstream JSON can
	// carry them literally. Exercise the aggregator's escaping of those too.
	return strings.ReplaceAll(strings.ReplaceAll(strings.TrimSpace(out.String()), `\u2028`, "\u2028"), `\u2029`, "\u2029")
}

func TestAggregateStreamedChoiceStructureBudget(t *testing.T) {
	var choices []map[string]any
	for i := 0; i < 100; i++ {
		choices = append(choices, map[string]any{"index": i, "delta": map[string]any{}, "finish_reason": "stop"})
	}
	chunk, err := json.Marshal(map[string]any{"choices": choices})
	if err != nil {
		t.Fatal(err)
	}
	wire := sseBody(string(chunk))
	aggregateTestLimit(t, int64(len(wire)))
	resp, err := (&Server{}).aggregateStreamed(&http.Response{Body: io.NopCloser(strings.NewReader(wire))})
	if err == nil || !strings.Contains(err.Error(), "aggregated upstream response exceeds") || resp != nil {
		t.Fatalf("empty choices escaped the accumulated output budget: resp=%v err=%v", resp, err)
	}
}

func TestAggregateStreamedRejectsLargeStructuresBeforeDecode(t *testing.T) {
	empty := func(n int) string { return "[" + strings.TrimSuffix(strings.Repeat("{},", n), ",") + "]" }
	for name, chunk := range map[string]string{
		"choices":            `{"choices":` + empty(1000) + `}`,
		"folded choices":     `{"ChOiCeS":` + empty(1000) + `}`,
		"escaped choices":    `{"cho\u0069ces":` + empty(1000) + `}`,
		"tools":              `{"choices":[{"delta":{"tool_calls":` + empty(1000) + `}}]}`,
		"folded tools":       `{"choices":[{"DeLtA":{"ToOl_CaLlS":` + empty(1000) + `}}]}`,
		"duplicate choices":  `{"choices":` + empty(40) + `,"CHOICES":` + empty(40) + `}`,
		"duplicate tools":    `{"choices":[{"delta":{"tool_calls":` + empty(60) + `,"TOOL_CALLS":` + empty(60) + `}}]}`,
		"duplicate delta":    `{"choices":[{"delta":{"tool_calls":` + empty(60) + `},"DELTA":{"tool_calls":` + empty(60) + `}}]}`,
		"escaped tool field": `{"choices":[{"delta":{"tool\u005fcalls":` + empty(1000) + `}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			const limit = 4096
			aggregateTestLimit(t, limit)
			wire := sseBody(chunk)
			if len(wire) >= limit {
				t.Fatal("fixture must fit the input budget to exercise the pre-decode structure budget")
			}
			resp, err := (&Server{}).aggregateStreamed(&http.Response{Body: io.NopCloser(strings.NewReader(wire))})
			if err == nil || !strings.Contains(err.Error(), "aggregated upstream response exceeds") || resp != nil {
				t.Fatalf("large typed slice reached semantic validation instead of the structure budget: resp=%v err=%v", resp, err)
			}
		})
	}
	// An ignored upstream extension does not allocate the typed choices slice.
	// Do not charge its coincidentally named nested fields as completion choices.
	t.Run("unrelated nested choices", func(t *testing.T) {
		aggregateTestLimit(t, 4096)
		wire := sseBody(`{"metadata":{"choices":` + empty(1000) + `},"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`)
		resp, err := (&Server{}).aggregateStreamed(&http.Response{Body: io.NopCloser(strings.NewReader(wire))})
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	})
	t.Run("standard JSON depth limit", func(t *testing.T) {
		aggregateTestLimit(t, 64<<10)
		wire := sseBody(`{"metadata":` + strings.Repeat("[", 15000) + "0" + strings.Repeat("]", 15000) + `,"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`)
		resp, err := (&Server{}).aggregateStreamed(&http.Response{Body: io.NopCloser(strings.NewReader(wire))})
		if err == nil || !strings.Contains(err.Error(), "invalid upstream SSE JSON") || resp != nil {
			t.Fatalf("preflight bypassed the JSON decoder nesting limit: resp=%v err=%v", resp, err)
		}
	})
}

func TestAggregateStreamedSizeLimitsFallBack(t *testing.T) {
	for _, route := range []string{"/v1/chat/completions", "/v1/auto", "/v1/responses"} {
		for _, fault := range []string{"input", "output"} {
			t.Run(route+"/"+fault, func(t *testing.T) {
				aggregateTestLimit(t, 512)
				var badHits, goodHits atomic.Int32
				bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					badHits.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					if fault == "input" {
						io.WriteString(w, "data: "+`{"choices":[{"delta":{"content":"partial"},"finish_reason":"stop"}]}`+"\n\n"+":"+strings.Repeat("x", 1024))
						return
					}
					io.WriteString(w, sseBody(`{"choices":[{"delta":{"content":"`+strings.Repeat("<", 100)+`"},"finish_reason":"stop"}]}`))
				}))
				defer bad.Close()
				good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					goodHits.Add(1)
					io.WriteString(w, `{"choices":[{"message":{"content":"backup"},"finish_reason":"stop"}]}`)
				}))
				defer good.Close()
				s, _ := testServer(t, []config.Provider{
					{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A", StreamOnly: true},
					{Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B"},
				}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "a"}, {Provider: "b", Model: "b"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, walkRequest(route, context.Background()))
				if w.Code != http.StatusOK || badHits.Load() != 1 || goodHits.Load() != 1 || !strings.Contains(w.Body.String(), "backup") || strings.Contains(w.Body.String(), "partial") {
					t.Fatalf("oversized SSE did not fall back cleanly: status=%d hits=%d/%d body=%s", w.Code, badHits.Load(), goodHits.Load(), w.Body)
				}
				if s.pools["a"].Keys()[0].Failures != 0 {
					t.Fatal("an oversized model response quarantined a healthy credential")
				}
			})
		}
	}
}

func TestAggregateStreamedSizeLimitWithoutFallback(t *testing.T) {
	for _, route := range []string{"/v1/chat/completions", "/v1/auto", "/v1/responses"} {
		t.Run(route, func(t *testing.T) {
			aggregateTestLimit(t, 512)
			var hits atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, sseBody(`{"choices":[{"delta":{"content":"`+strings.Repeat("<", 100)+`"},"finish_reason":"stop"}]}`))
			}))
			defer up.Close()
			s, _ := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: up.URL, KeyEnv: "A", StreamOnly: true}},
				[]config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "a"}}}}, map[string]string{"A": "dummy"})
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, walkRequest(route, context.Background()))
			if w.Code != http.StatusBadGateway || hits.Load() != 1 || !strings.Contains(w.Body.String(), "upstream") || strings.Contains(w.Body.String(), "chat.completion") {
				t.Fatalf("oversized aggregation became a successful response: status=%d hits=%d body=%s", w.Code, hits.Load(), w.Body)
			}
		})
	}
}
