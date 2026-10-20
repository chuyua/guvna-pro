package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestChoiceContractNonStreamIndices(t *testing.T) {
	for _, tc := range []struct {
		name    string
		choices string
		n       int
		failed  bool
	}{
		{"complete out of order", `{"index":1,"message":{"content":"b"}},{"index":0,"message":{"content":"a"}}`, 2, false},
		{"missing choice", `{"index":0,"message":{"content":"a"}}`, 2, true},
		{"extra choice", `{"index":0,"message":{"content":"a"}},{"index":1,"message":{"content":"b"}},{"index":2,"message":{"content":"c"}}`, 2, true},
		{"negative index", `{"index":-1,"message":{"content":"a"}},{"index":1,"message":{"content":"b"}}`, 2, true},
		{"duplicate index", `{"index":0,"message":{"content":"a"}},{"index":0,"message":{"content":"b"}}`, 2, true},
		{"out of range", `{"index":0,"message":{"content":"a"}},{"index":2,"message":{"content":"b"}}`, 2, true},
		{"missing index", `{"message":{"content":"a"}},{"index":1,"message":{"content":"b"}}`, 2, true},
		{"null index", `{"index":null,"message":{"content":"a"}},{"index":1,"message":{"content":"b"}}`, 2, true},
		{"string index", `{"index":"0","message":{"content":"a"}},{"index":1,"message":{"content":"b"}}`, 2, true},
		{"single without index", `{"message":{"content":"a"}}`, 1, false},
		{"single preserves extra", `{"index":0,"message":{"content":"a"}},{"index":1,"message":{"content":"b"}}`, 1, false},
		{"omitted n preserves extra", `{"index":0,"message":{"content":"a"}},{"index":1,"message":{"content":"b"}}`, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateChatCompletion([]byte(`{"choices":[`+tc.choices+`]}`), tc.n)
			if (err != nil) != tc.failed {
				t.Fatalf("validation error=%v, want failure=%v", err, tc.failed)
			}
		})
	}
}

func TestChoiceContractDefaultsMatchPayload(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		value      any
		want       int
	}{
		{"missing", `{"messages":[{"content":"hi"}]}`, 2, 2},
		{"client wins", `{"n":1}`, 2, 1},
		{"client zero", `{"n":0}`, 2, 0},
		{"client null", `{"n":null}`, 2, 0},
		{"key case follows merge", `{"N":4}`, 2, 2},
		{"json number", `{}`, json.Number("2"), 2},
		{"float integer", `{}`, float64(2), 2},
		{"invalid default", `{}`, "2", 0},
		{"unmarshalable default", `{}`, func() {}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var req chatRequest
			if err := json.Unmarshal([]byte(tc.body), &req); err != nil {
				t.Fatal(err)
			}
			params := map[string]any{"n": tc.value}
			got := withChoiceDefault(req, []byte(tc.body), params)
			if got.N != tc.want || got.Model != req.Model || got.Stream != req.Stream {
				t.Fatalf("effective request=%+v, want n=%d", got, tc.want)
			}
			if tc.name == "invalid default" || tc.name == "unmarshalable default" {
				return // Invalid provider parameters are still left to the upstream.
			}
			var payload struct {
				N json.RawMessage `json:"n"`
			}
			if err := json.Unmarshal(mergeParams([]byte(tc.body), params), &payload); err != nil {
				t.Fatal(err)
			}
			var actual int
			if err := json.Unmarshal(payload.N, &actual); err != nil || actual != got.N {
				t.Fatalf("expected count differs from forwarded payload: n=%s, request=%+v, error=%v", payload.N, got, err)
			}
		})
	}
}

func TestChoiceContractAcrossChatAndAuto(t *testing.T) {
	for _, route := range []string{"chat", "auto"} {
		for _, mode := range []string{"json", "aggregate", "stream"} {
			for _, tc := range []struct {
				name                   string
				clientN                *int
				defaultA, defaultB     int
				returnedA, returnedB   int
				forwardedA, forwardedB int
				fallback               bool
			}{
				{"chain default missing", nil, 2, 3, 1, 3, 2, 3, true},
				{"explicit missing", choiceContractInt(2), 3, 4, 1, 2, 2, 2, true},
				{"duplicate index", nil, 2, 3, 2, 3, 2, 3, true},
				{"client wins", choiceContractInt(1), 2, 3, 1, 3, 1, 1, false},
				{"chain default complete", nil, 2, 3, 2, 3, 2, 3, false},
			} {
				t.Run(route+"/"+mode+"/"+tc.name, func(t *testing.T) {
					var hitsA, hitsB, nA, nB atomic.Int32
					upstream := func(label string, returned int, hits, n *atomic.Int32) *httptest.Server {
						return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							hits.Add(1)
							var body struct {
								N int `json:"n"`
							}
							if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
								t.Errorf("read upstream request: %v", err)
								w.WriteHeader(http.StatusBadRequest)
								return
							}
							n.Store(int32(body.N))
							wire := choiceContractResponse(mode, returned, label)
							if tc.name == "duplicate index" && label == "first" {
								wire = strings.Replace(wire, `"index":1`, `"index":0`, 1)
							}
							io.WriteString(w, wire)
						}))
					}
					bad := upstream("first", tc.returnedA, &hitsA, &nA)
					defer bad.Close()
					good := upstream("backup", tc.returnedB, &hitsB, &nB)
					defer good.Close()
					providers := []config.Provider{{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A", StreamOnly: mode == "aggregate"}, {Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B", StreamOnly: mode == "aggregate"}}
					stepA := config.Step{Provider: "a", Model: "m-a", Params: map[string]any{"n": tc.defaultA}}
					stepB := config.Step{Provider: "b", Model: "m-b", Params: map[string]any{"n": tc.defaultB}}
					chains := []config.Chain{{Name: "main", Steps: []config.Step{stepA, stepB}}}
					path, model := "/v1/chat/completions", "main"
					if route == "auto" {
						path, model = "/v1/auto", "auto"
						chains = []config.Chain{{Name: "fam-a", Steps: []config.Step{stepA}}, {Name: "fam-b", Steps: []config.Step{stepB}}}
					}
					s, _ := testServerDefault(t, providers, chains, map[string]string{"A": "dummy", "B": "dummy"}, "")
					request := map[string]any{"model": model, "stream": mode == "stream", "messages": []any{map[string]any{"role": "user", "content": "hi"}}}
					if tc.clientN != nil {
						request["n"] = *tc.clientN
					}
					raw, err := json.Marshal(request)
					if err != nil {
						t.Fatal(err)
					}
					r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(raw)))
					r.Header.Set("Authorization", "Bearer client-1")
					w := httptest.NewRecorder()
					s.Handler().ServeHTTP(w, r)
					body := w.Body.String()
					failedStream := mode == "stream" && tc.fallback
					wantB := int32(0)
					if tc.fallback && !failedStream {
						wantB = 1
					}
					if w.Code != http.StatusOK || hitsA.Load() != 1 || hitsB.Load() != wantB || nA.Load() != int32(tc.forwardedA) || wantB != 0 && nB.Load() != int32(tc.forwardedB) {
						t.Fatalf("wrong outcome: status=%d hits=%d/%d n=%d/%d body=%s", w.Code, hitsA.Load(), hitsB.Load(), nA.Load(), nB.Load(), body)
					}
					if failedStream {
						if !strings.Contains(body, `"error"`) || strings.Contains(body, "[DONE]") {
							t.Fatalf("incomplete stream marked successful: %s", body)
						}
					} else {
						label := "first"
						if tc.fallback {
							label = "backup"
						}
						if !strings.Contains(body, label) || strings.Contains(body, `"error"`) || tc.fallback && strings.Contains(body, "first") {
							t.Fatalf("wrong completion committed: %s", body)
						}
					}
					for _, state := range s.hlth.Snapshot() {
						want := 0
						if state.Provider == "a" && tc.fallback {
							want = 1
						}
						if state.Failures != want {
							t.Fatalf("wrong model attribution: %+v", state)
						}
					}
				})
			}
		}
	}
}

func choiceContractInt(n int) *int { return &n }

func choiceContractResponse(mode string, count int, label string) string {
	choices := make([]any, 0, count)
	for i := 0; i < count; i++ {
		field := "message"
		if mode != "json" {
			field = "delta"
		}
		choices = append(choices, map[string]any{"index": i, field: map[string]any{"content": fmt.Sprintf("%s-%d", label, i)}, "finish_reason": "stop"})
	}
	raw, _ := json.Marshal(map[string]any{"choices": choices})
	if mode == "json" {
		return string(raw)
	}
	return sseBody(string(raw))
}
