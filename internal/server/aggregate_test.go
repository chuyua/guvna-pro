package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/creamy-ghost/guvna/internal/config"
)

func sseBody(chunks ...string) string {
	var b strings.Builder
	for _, c := range chunks {
		b.WriteString("data: " + c + "\n\n")
	}
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

func TestAggregateStreamedJoinsChunks(t *testing.T) {
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader(sseBody(
			`{"id":"c1","created":100,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"He"}}],"usage":{}}`,
			`{"id":"c1","created":100,"model":"m","choices":[{"index":0,"delta":{"content":"llo"}}]}`,
			`{"id":"c1","created":100,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":null}]}`,
			`{"id":"c1","created":100,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
		))),
	}
	s := &Server{}
	agg, err := s.aggregateStreamed(resp)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(agg.Body)
	var out struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Role             string `json:"role"`
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("aggregated body is not valid JSON: %v\n%s", err, body)
	}
	if out.ID != "c1" || out.Model != "m" {
		t.Fatalf("id/model = %q/%q, want c1/m", out.ID, out.Model)
	}
	if len(out.Choices) != 1 || out.Choices[0].Message.Content != "Hello" {
		t.Fatalf("content = %q, want Hello", out.Choices[0].Message.Content)
	}
	if out.Choices[0].FinishReason != "stop" {
		t.Fatalf("finish_reason = %q, want stop", out.Choices[0].FinishReason)
	}
	if out.Choices[0].Message.ReasoningContent != "" {
		t.Fatalf("unexpected reasoning_content %q", out.Choices[0].Message.ReasoningContent)
	}
	if out.Usage.TotalTokens != 5 {
		t.Fatalf("usage.total_tokens = %d, want 5", out.Usage.TotalTokens)
	}
	if got := agg.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("content-type = %q, want application/json", got)
	}
}

func TestAggregateStreamedCollectsReasoning(t *testing.T) {
	// reasoning_content (DeepSeek style) and reasoning (Mimo style) both land
	// in the aggregated message.
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(sseBody(
			`{"id":"c2","created":1,"model":"m","choices":[{"index":0,"delta":{"reasoning_content":"think"}}]}`,
			`{"id":"c2","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"ans","reasoning":"ing"}}]}`,
			`{"id":"c2","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		))),
	}
	s := &Server{}
	agg, err := s.aggregateStreamed(resp)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(agg.Body)
	var out struct {
		Choices []struct {
			Message struct {
				Content          string `json:"content"`
				ReasoningContent string `json:"reasoning_content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	m := out.Choices[0].Message
	if m.Content != "ans" || m.ReasoningContent != "thinking" {
		t.Fatalf("content/reasoning = %q/%q, want ans/thinking", m.Content, m.ReasoningContent)
	}
}

func TestAggregateStreamedRejectsTruncatedStream(t *testing.T) {
	// No finish_reason before [DONE]: the stream was cut short and must be
	// reported as an error so the walk marks the step down.
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(sseBody(
			`{"id":"c3","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"partial"}}]}`,
		))),
	}
	s := &Server{}
	if _, err := s.aggregateStreamed(resp); err == nil {
		t.Fatal("want error for stream without finish_reason, got nil")
	}
}

func TestAggregateStreamedRejectsInvalidPayloads(t *testing.T) {
	for name, chunks := range map[string][]string{
		"bad JSON before finish": {`{"choices":`, `{"choices":[{"delta":{},"finish_reason":"stop"}]}`},
		"error before finish":    {`{"error":{"message":"broken"}}`, `{"choices":[{"delta":{},"finish_reason":"stop"}]}`},
		"null":                   {`null`, `{"choices":[{"delta":{},"finish_reason":"stop"}]}`},
		"unknown finish":         {`{"choices":[{"delta":{},"finish_reason":"surprise"}]}`},
		"empty tools":            {`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`},
		"missing tool identity":  {`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`},
		"missing tool index":     {`{"choices":[{"delta":{"tool_calls":[{"id":"call","type":"function","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`},
		"invalid tool arguments": {`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"f","arguments":"{"}}]},"finish_reason":"tool_calls"}]}`},
	} {
		t.Run(name, func(t *testing.T) {
			resp := &http.Response{Body: io.NopCloser(strings.NewReader(sseBody(chunks...)))}
			if _, err := (&Server{}).aggregateStreamed(resp); err == nil {
				t.Fatal("invalid stream reported success")
			}
		})
	}
}

func TestAggregateStreamedDispatchesPendingEOF(t *testing.T) {
	for _, reason := range []string{"stop", "length"} {
		t.Run(reason, func(t *testing.T) {
			wire := "data: " + `{"choices":[{"delta":{"content":"answer"},"finish_reason":"` + reason + `"}]}`
			resp := &http.Response{Body: io.NopCloser(strings.NewReader(wire))}
			agg, err := (&Server{}).aggregateStreamed(resp)
			if err != nil {
				t.Fatal(err)
			}
			defer agg.Body.Close()
			body, _ := io.ReadAll(agg.Body)
			if !strings.Contains(string(body), `"content":"answer"`) || !strings.Contains(string(body), `"finish_reason":"`+reason+`"`) {
				t.Fatalf("pending event lost: %s", body)
			}
		})
	}
}

func TestAggregateStreamedCollectsInterleavedToolCalls(t *testing.T) {
	chunks := []string{
		`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_","type":"fun","function":{"name":"get_","arguments":"{\"b\":"}},{"index":0,"id":"first","type":"function","function":{"name":"a","arguments":"{\"a\":"}}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}},{"index":1,"id":"second","type":"ction","function":{"name":"b","arguments":"2}"}}]},"finish_reason":"tool_calls"}]}`,
	}
	// Deliberately omit the blank line and DONE after the final event.
	wire := "data: " + chunks[0] + "\n\ndata: " + chunks[1]
	agg, err := (&Server{}).aggregateStreamed(&http.Response{Body: io.NopCloser(strings.NewReader(wire))})
	if err != nil {
		t.Fatal(err)
	}
	defer agg.Body.Close()
	body, _ := io.ReadAll(agg.Body)
	var got struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					ID       string `json:"id"`
					Type     string `json:"type"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Choices) != 1 || len(got.Choices[0].Message.ToolCalls) != 2 || got.Choices[0].FinishReason != "tool_calls" {
		t.Fatalf("tool calls missing: %s", body)
	}
	calls := got.Choices[0].Message.ToolCalls
	if calls[0].ID != "first" || calls[0].Function.Arguments != `{"a":1}` || calls[1].ID != "call_second" || calls[1].Type != "function" || calls[1].Function.Name != "get_b" || calls[1].Function.Arguments != `{"b":2}` {
		t.Fatalf("tool fragments or index order lost: %s", body)
	}
}

func TestForceStreamBodyAddsStreamAndUsage(t *testing.T) {
	out := forceStreamBody([]byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["stream"] != true {
		t.Fatalf("stream = %v, want true", m["stream"])
	}
	so, _ := m["stream_options"].(map[string]any)
	if so == nil || so["include_usage"] != true {
		t.Fatalf("stream_options = %v, want include_usage true", m["stream_options"])
	}
	if _, ok := m["messages"]; !ok {
		t.Fatal("messages lost in rewrite")
	}
}

func TestForceStreamBodyPassesThroughBadJSON(t *testing.T) {
	in := []byte(`not json`)
	if got := forceStreamBody(in); string(got) != string(in) {
		t.Fatalf("bad JSON rewritten: %q", got)
	}
}

// streamingOnlyUpstream answers only SSE, and fails the request if the client
// body did not ask for streaming — the shape of the real yiyanz upstream
// (non-stream POST → 502, stream → valid SSE).
func streamingOnlyUpstream(t *testing.T, content string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body struct {
			Stream        bool `json:"stream"`
			StreamOptions struct {
				IncludeUsage bool `json:"include_usage"`
			} `json:"stream_options"`
		}
		if err := json.Unmarshal(raw, &body); err != nil || !body.Stream {
			http.Error(w, `{"error":{"message":"billing exploded"}}`, http.StatusBadGateway)
			return
		}
		if !body.StreamOptions.IncludeUsage {
			t.Error("upstream expected stream_options.include_usage for usage capture")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: "+`{"id":"u1","created":9,"model":"up","choices":[{"index":0,"delta":{"role":"assistant","content":"`+content+`"}}]}`+"\n\n")
		io.WriteString(w, "data: "+`{"id":"u1","created":9,"model":"up","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`+"\n\n")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
}

func TestStreamOnlyAggregatesForChatClient(t *testing.T) {
	up := streamingOnlyUpstream(t, "aggregated")
	providers := []config.Provider{{Name: "so", Type: "openai", BaseURL: up.URL, KeyEnv: "KEY_SO", StreamOnly: true}}
	chains := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "so", Model: "m-so"}}}}
	_, ts := testServer(t, providers, chains, map[string]string{"KEY_SO": "k"})

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"main","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer client-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	var out struct {
		Object  string `json:"object"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("client-facing body is not a chat.completion: %v\n%s", err, body)
	}
	if out.Object != "chat.completion" || out.Choices[0].Message.Content != "aggregated" ||
		out.Choices[0].FinishReason != "stop" || out.Usage.TotalTokens != 2 {
		t.Fatalf("aggregation mismatch: %s", body)
	}
}

func TestStreamOnlyAggregatesForAutoClient(t *testing.T) {
	up := streamingOnlyUpstream(t, "auto-agg")
	providers := []config.Provider{{Name: "so", Type: "openai", BaseURL: up.URL, KeyEnv: "KEY_SO", StreamOnly: true}}
	chains := []config.Chain{{Name: "main", Steps: []config.Step{{Provider: "so", Model: "m-so"}}}}
	_, ts := autoServer(t, providers, chains, map[string]string{"KEY_SO": "k"})

	resp := doAuto(t, ts, nil)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "auto-agg") || !strings.Contains(string(body), `"finish_reason":"stop"`) {
		t.Fatalf("aggregated body mismatch: %s", body)
	}
}
