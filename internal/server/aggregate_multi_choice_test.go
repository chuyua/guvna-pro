package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

type aggregatedChoices struct {
	Choices []struct {
		Index   int `json:"index"`
		Message struct {
			Content   string `json:"content"`
			Reasoning string `json:"reasoning_content"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
		Finish string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		Prompt     int `json:"prompt_tokens"`
		Completion int `json:"completion_tokens"`
		Total      int `json:"total_tokens"`
	} `json:"usage"`
}

func aggregateChoices(t *testing.T, chunks ...string) aggregatedChoices {
	t.Helper()
	agg, err := (&Server{}).aggregateStreamed(&http.Response{Body: io.NopCloser(strings.NewReader(sseBody(chunks...)))})
	if err != nil {
		t.Fatal(err)
	}
	defer agg.Body.Close()
	var got aggregatedChoices
	if err := json.NewDecoder(agg.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestAggregateStreamedMultipleChoicesInterleaveAndSort(t *testing.T) {
	got := aggregateChoices(t,
		`{"choices":[{"index":1,"delta":{"content":"second-","reasoning":"think-1"}},{"index":0,"delta":{"content":"first-","reasoning_content":"think-0"}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"answer","reasoning":"done-0"},"finish_reason":"stop"}]}`,
		`{"choices":[{"index":1,"delta":{"content":"partial","reasoning_content":"done-1"},"finish_reason":"length"}]}`,
		`{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":7,"total_tokens":10}}`,
	)
	if len(got.Choices) != 2 {
		t.Fatalf("choices=%+v, want both choices", got.Choices)
	}
	a, b := got.Choices[0], got.Choices[1]
	if a.Index != 0 || a.Message.Content != "first-answer" || a.Message.Reasoning != "think-0done-0" || a.Finish != "stop" ||
		b.Index != 1 || b.Message.Content != "second-partial" || b.Message.Reasoning != "think-1done-1" || b.Finish != "length" {
		t.Fatalf("choice data mixed or lost: %+v", got.Choices)
	}
	if got.Usage.Prompt != 3 || got.Usage.Completion != 7 || got.Usage.Total != 10 {
		t.Fatalf("global usage lost: %+v", got.Usage)
	}
}

func TestAggregateStreamedMultipleChoicesKeepToolsLocal(t *testing.T) {
	got := aggregateChoices(t,
		`{"choices":[{"index":1,"delta":{"tool_calls":[{"index":0,"id":"other","type":"function","function":{"name":"other","arguments":"{\"v\":"}}]}},{"index":0,"delta":{"tool_calls":[{"index":1,"id":"second","type":"function","function":{"name":"b","arguments":"{\"b\":"}},{"index":0,"id":"first","type":"function","function":{"name":"a","arguments":"{\"a\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}},{"index":1,"function":{"arguments":"2}"}}]},"finish_reason":"tool_calls"},{"index":1,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"3}"}}]},"finish_reason":"tool_calls"}]}`,
	)
	if len(got.Choices) != 2 || len(got.Choices[0].Message.ToolCalls) != 2 || len(got.Choices[1].Message.ToolCalls) != 1 {
		t.Fatalf("tool call counts=%+v", got.Choices)
	}
	a, b := got.Choices[0].Message.ToolCalls, got.Choices[1].Message.ToolCalls
	if a[0].ID != "first" || a[0].Function.Arguments != `{"a":1}` || a[1].ID != "second" || a[1].Function.Arguments != `{"b":2}` || b[0].ID != "other" || b[0].Function.Arguments != `{"v":3}` {
		t.Fatalf("tool indices mixed across choices: %+v", got.Choices)
	}
}

func TestAggregateStreamedMultipleChoicesMixToolsAndText(t *testing.T) {
	got := aggregateChoices(t,
		`{"choices":[{"index":1,"delta":{"content":"answer"},"finish_reason":"stop"},{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`,
	)
	if len(got.Choices) != 2 || got.Choices[0].Finish != "tool_calls" || len(got.Choices[0].Message.ToolCalls) != 1 || got.Choices[1].Message.Content != "answer" || len(got.Choices[1].Message.ToolCalls) != 0 {
		t.Fatalf("tool and text choices mixed: %+v", got.Choices)
	}
}

func TestAggregateStreamedRejectsConflictingOrIncompleteChoices(t *testing.T) {
	for name, chunks := range map[string][]string{
		"missing index in multi choice": {`{"choices":[{"delta":{"content":"a"},"finish_reason":"stop"},{"index":1,"delta":{"content":"b"},"finish_reason":"stop"}]}`},
		"negative index":                {`{"choices":[{"index":-1,"delta":{"content":"a"},"finish_reason":"stop"}]}`},
		"duplicate index in chunk":      {`{"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":"stop"},{"index":0,"delta":{"content":"b"},"finish_reason":"stop"}]}`},
		"unfinished second choice":      {`{"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":"stop"},{"index":1,"delta":{"content":"partial"}}]}`},
		"conflicting finish":            {`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`, `{"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}`},
		"data after finish":             {`{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`, `{"choices":[{"index":0,"delta":{"content":"late"}}]}`},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (&Server{}).aggregateStreamed(&http.Response{Body: io.NopCloser(strings.NewReader(sseBody(chunks...)))}); err == nil {
				t.Fatal("invalid multi-choice stream reported success")
			}
		})
	}
}

func TestAggregateStreamedHonorsRequestedChoiceCount(t *testing.T) {
	for _, tc := range []struct {
		name, chunk string
		failed      bool
	}{
		{"missing requested choice", `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`, true},
		{"out of range choice", `{"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":2,"delta":{},"finish_reason":"stop"}]}`, true},
		{"missing index", `{"choices":[{"delta":{},"finish_reason":"stop"}]}`, true},
		{"all requested choices", `{"choices":[{"index":1,"delta":{"content":"b"},"finish_reason":"stop"},{"index":0,"delta":{"content":"a"},"finish_reason":"stop"}]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			agg, err := (&Server{}).aggregateStreamed(&http.Response{Body: io.NopCloser(strings.NewReader(sseBody(tc.chunk)))}, 2)
			if (err != nil) != tc.failed {
				t.Fatalf("aggregate error=%v want failure=%v", err, tc.failed)
			}
			if agg != nil {
				agg.Body.Close()
			}
		})
	}
}

func TestStreamOnlyIncompleteChoiceFallsBackBeforeCommit(t *testing.T) {
	var badHits, goodHits atomic.Int32
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		badHits.Add(1)
		io.WriteString(w, sseBody(`{"choices":[{"index":0,"delta":{"content":"wrong"},"finish_reason":"stop"},{"index":1,"delta":{"content":"partial"}}]}`))
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		goodHits.Add(1)
		io.WriteString(w, `{"choices":[{"index":0,"message":{"content":"backup"},"finish_reason":"stop"}]}`)
	}))
	defer good.Close()
	s, ts := testServer(t, []config.Provider{{Name: "a", Type: "openai", BaseURL: bad.URL, KeyEnv: "A", StreamOnly: true}, {Name: "b", Type: "openai", BaseURL: good.URL, KeyEnv: "B"}}, []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "a", Model: "a"}, {Provider: "b", Model: "b"}}}}, map[string]string{"A": "dummy", "B": "dummy"})
	resp := doChat(t, ts, "main", false)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(body), "backup") || strings.Contains(string(body), "wrong") || badHits.Load() != 1 || goodHits.Load() != 1 {
		t.Fatalf("incomplete choice was committed: status=%d hits=%d/%d body=%s", resp.StatusCode, badHits.Load(), goodHits.Load(), body)
	}
	for _, st := range s.hlth.Snapshot() {
		if st.Provider == "a" && st.Failures != 1 {
			t.Fatalf("incomplete aggregate failures=%d, want 1", st.Failures)
		}
	}
}
