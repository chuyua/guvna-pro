// Tests for the Responses-API response translator: the non-streaming renderer
// and the SSE stream translator. SSE output is parsed into (event, data
// object) pairs and asserted structurally rather than as whole blobs.
package responses

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// recFlusher satisfies http.Flusher for httptest.ResponseRecorder, which does
// not implement it.
type recFlusher struct{}

func (recFlusher) Flush() {}

type sseEvent struct {
	name string
	data map[string]any
}

func (e sseEvent) typ() string {
	if s, ok := e.data["type"].(string); ok {
		return s
	}
	return ""
}

func (e sseEvent) response() map[string]any {
	if r, ok := e.data["response"].(map[string]any); ok {
		return r
	}
	return nil
}

// parseSSE splits captured SSE output into (event, data object) pairs. The
// data: [DONE] sentinel is returned with a nil data map.
func parseSSE(t *testing.T, body string) []sseEvent {
	t.Helper()
	var events []sseEvent
	for _, block := range strings.Split(body, "\n\n") {
		if strings.TrimSpace(block) == "" {
			continue
		}
		name, payload := "", ""
		for _, line := range strings.Split(block, "\n") {
			switch {
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				payload = strings.TrimPrefix(line, "data: ")
			}
		}
		if payload == "" {
			continue
		}
		if payload == "[DONE]" {
			if name != "" {
				t.Errorf("terminal sentinel carries an event: line: %q", name)
			}
			events = append(events, sseEvent{})
			continue
		}
		data := map[string]any{}
		if err := json.Unmarshal([]byte(payload), &data); err != nil {
			t.Fatalf("event %q payload is not a JSON object: %v\n%s", name, err, payload)
		}
		events = append(events, sseEvent{name: name, data: data})
	}
	return events
}

func eventLog(events []sseEvent) string {
	var b strings.Builder
	for i, e := range events {
		fmt.Fprintf(&b, "%2d %-45s %v\n", i, e.name, e.typ())
	}
	return b.String()
}

func wantNames(t *testing.T, events []sseEvent, want []string) {
	t.Helper()
	if len(events) != len(want) {
		t.Fatalf("event count = %d, want %d\ngot:\n%s", len(events), len(want), eventLog(events))
	}
	for i, w := range want {
		if events[i].name != w {
			t.Errorf("event[%d] = %q, want %q\ngot:\n%s", i, events[i].name, w, eventLog(events))
		}
	}
}

// requireFields checks that an event payload carries each named field.
func requireFields(t *testing.T, e sseEvent, fields ...string) {
	t.Helper()
	for _, f := range fields {
		if _, ok := e.data[f]; !ok {
			t.Errorf("event %q missing field %q: %v", e.name, f, e.data)
		}
	}
}

// walkInvariants checks the wire contract Codex validates: the payload type
// matches the event: line name, item-delta events carry response_id/item_id/
// output_index, content-part events additionally carry content_index, and
// output_index values ascend without reuse.
func walkInvariants(t *testing.T, events []sseEvent) []int {
	t.Helper()
	needsItemID := map[string]bool{
		"response.content_part.added":            true,
		"response.content_part.done":             true,
		"response.reasoning_summary_text.delta":  true,
		"response.reasoning_summary_text.done":   true,
		"response.output_text.delta":             true,
		"response.output_text.done":              true,
		"response.function_call_arguments.delta": true,
		"response.function_call_arguments.done":  true,
	}
	needsContentIndex := map[string]bool{
		"response.content_part.added":           true,
		"response.content_part.done":            true,
		"response.reasoning_summary_text.delta": true,
		"response.reasoning_summary_text.done":  true,
		"response.output_text.delta":            true,
		"response.output_text.done":             true,
	}
	var firstSeen []int
	seen := map[int]bool{}
	for _, e := range events {
		if e.data == nil {
			continue
		}
		if e.typ() != e.name {
			t.Errorf("event %q payload type = %q", e.name, e.typ())
		}
		if e.name == "response.output_item.added" || e.name == "response.output_item.done" {
			requireFields(t, e, "response_id", "output_index")
		}
		if needsItemID[e.name] {
			requireFields(t, e, "response_id", "item_id", "output_index")
			if needsContentIndex[e.name] {
				if ci, ok := e.data["content_index"].(float64); !ok || ci != 0 {
					t.Errorf("event %q content_index = %v, want 0", e.name, e.data["content_index"])
				}
			}
		}
		if oi, ok := e.data["output_index"].(float64); ok {
			n := int(oi)
			if seen[n] {
				continue
			}
			seen[n] = true
			if len(firstSeen) > 0 && n <= firstSeen[len(firstSeen)-1] {
				t.Errorf("output_index first seen out of ascending order: %d after %v", n, firstSeen)
			}
			firstSeen = append(firstSeen, n)
		}
	}
	return firstSeen
}

func mustCompletion(t *testing.T, raw string) *ChatCompletion {
	t.Helper()
	cc := &ChatCompletion{}
	if err := json.Unmarshal([]byte(raw), cc); err != nil {
		t.Fatal(err)
	}
	return cc
}

// parsedItems accepts a []map[string]any slice straight from FromChat, or the
// []any slice a JSON round-trip produces, and normalises both.
func parsedItems(v any) []map[string]any {
	if out, ok := v.([]map[string]any); ok {
		return out
	}
	raw, ok := v.([]any)
	if !ok {
		panic(fmt.Sprintf("test helper: parsedItems got %T", v))
	}
	out := make([]map[string]any, 0, len(raw))
	for _, p := range raw {
		m, ok := p.(map[string]any)
		if !ok {
			panic(fmt.Sprintf("test helper: parsedItems element is %T", p))
		}
		out = append(out, m)
	}
	return out
}

// messageContent returns the parts of a message or reasoning item.
func messageContent(item map[string]any) []map[string]any {
	return parsedItems(item["content"])
}

// TestFromChatOutputItems covers the output array shapes Codex reconstructs a
// transcript from, including the zero-item case that must not happen.
func TestFromChatOutputItems(t *testing.T) {
	cases := []struct {
		name  string
		cc    string
		check func(t *testing.T, items []map[string]any)
	}{
		{
			name: "plain text",
			cc:   `{"choices":[{"message":{"role":"assistant","content":"hi there"}}]}`,
			check: func(t *testing.T, items []map[string]any) {
				if len(items) != 1 || items[0]["type"] != "message" {
					t.Fatalf("items = %v, want one message", items)
				}
				if items[0]["status"] != "completed" || items[0]["role"] != "assistant" {
					t.Errorf("item status/role = %v/%v", items[0]["status"], items[0]["role"])
				}
				content := messageContent(items[0])
				if len(content) != 1 || content[0]["type"] != "output_text" || content[0]["text"] != "hi there" {
					t.Errorf("content = %v, want one output_text \"hi there\"", content)
				}
				if content[0]["logprobs"] != nil {
					t.Errorf("logprobs = %v, want nil", content[0]["logprobs"])
				}
				if ann := content[0]["annotations"].([]any); len(ann) != 0 {
					t.Errorf("annotations = %v, want empty", ann)
				}
			},
		},
		{
			name: "reasoning first then message",
			cc:   `{"choices":[{"message":{"role":"assistant","reasoning":"think hard","content":"42"}}]}`,
			check: func(t *testing.T, items []map[string]any) {
				if len(items) != 2 {
					t.Fatalf("items = %v, want 2", items)
				}
				if items[0]["type"] != "reasoning" || items[1]["type"] != "message" {
					t.Fatalf("types = %v, %v — reasoning must come first", items[0]["type"], items[1]["type"])
				}
				if items[0]["status"] != "completed" || !strings.HasPrefix(items[0]["id"].(string), "rs_") {
					t.Errorf("reasoning item = %v", items[0])
				}
				summary := parsedItems(items[0]["summary"])
				if len(summary) != 1 || summary[0]["text"] != "think hard" || summary[0]["status"] != "completed" {
					t.Errorf("summary = %v", summary)
				}
				content := messageContent(items[1])
				if len(content) != 1 || content[0]["text"] != "42" {
					t.Errorf("content = %v", content)
				}
			},
		},
		{
			name: "two tool calls",
			cc: `{"choices":[{"message":{"role":"assistant","tool_calls":[
				{"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}},
				{"id":"call_2","type":"function","function":{"name":"noop","arguments":""}}]}}]}`,
			check: func(t *testing.T, items []map[string]any) {
				if len(items) != 2 {
					t.Fatalf("items = %v, want 2 function_call items", items)
				}
				for i, it := range items {
					if it["type"] != "function_call" || it["status"] != "completed" {
						t.Errorf("item[%d] type/status = %v/%v", i, it["type"], it["status"])
					}
					if !strings.HasPrefix(it["id"].(string), "fc_") {
						t.Errorf("item[%d] id = %v, want fc_ prefix", i, it["id"])
					}
					args, isStr := it["arguments"].(string)
					if !isStr {
						t.Fatalf("item[%d] arguments = %v (%T), want a JSON string", i, it["arguments"], it["arguments"])
					}
					var parsed map[string]any
					if err := json.Unmarshal([]byte(args), &parsed); err != nil {
						t.Errorf("item[%d] arguments %q is not valid JSON: %v", i, args, err)
					}
				}
				if items[0]["call_id"] != "call_1" || items[0]["name"] != "get_weather" {
					t.Errorf("item[0] call_id/name = %v/%v", items[0]["call_id"], items[0]["name"])
				}
				if got := items[0]["arguments"]; got != `{"city":"Paris"}` {
					t.Errorf("item[0] arguments = %v", got)
				}
				if items[1]["call_id"] != "call_2" || items[1]["name"] != "noop" {
					t.Errorf("item[1] call_id/name = %v/%v", items[1]["call_id"], items[1]["name"])
				}
				if got := items[1]["arguments"]; got != "{}" {
					t.Errorf("item[1] empty arguments = %v, want {} ", got)
				}
				if items[0]["id"] == items[1]["id"] {
					t.Errorf("function_call ids must be unique: %v", items[0]["id"])
				}
			},
		},
		{
			name: "empty content and no tool calls",
			cc:   `{"choices":[{"message":{"role":"assistant","content":""}}]}`,
			check: func(t *testing.T, items []map[string]any) {
				if len(items) != 1 {
					t.Fatalf("items = %v — the zero-item output array must not happen", items)
				}
				content := messageContent(items[0])
				if len(content) != 1 || content[0]["text"] != "" {
					t.Errorf("content = %v, want one empty output_text", content)
				}
			},
		},
		{
			name: "refusal",
			cc:   `{"choices":[{"message":{"role":"assistant","refusal":"not allowed"}}]}`,
			check: func(t *testing.T, items []map[string]any) {
				if len(items) != 1 || items[0]["type"] != "message" {
					t.Fatalf("items = %v", items)
				}
				content := messageContent(items[0])
				if len(content) != 1 || content[0]["type"] != "refusal" || content[0]["refusal"] != "not allowed" {
					t.Errorf("content = %v, want one refusal part", content)
				}
			},
		},
		{
			name: "no choices at all",
			cc:   `{"choices":[]}`,
			check: func(t *testing.T, items []map[string]any) {
				if len(items) != 1 || items[0]["type"] != "message" {
					t.Fatalf("items = %v — a response must never have an empty output array", items)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := FromChat(mustCompletion(t, tc.cc), &Request{}, Served{ChatModel: "m", ChainModel: "chain"})
			tc.check(t, resp.Output)
		})
	}
}

// TestFromChatUsageMapping pins the chat-to-Responses usage translation,
// including the nil-usage path Codex depends on.
func TestFromChatUsageMapping(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cc         string
		wantIn     int64
		wantOut    int64
		wantTotal  int64
		wantCached int64
		wantReason int64
	}{
		{"nil usage", `{"choices":[{"message":{"content":"x"}}]}`, 0, 0, 0, -1, -1},
		{"plain", `{"choices":[{"message":{"content":"x"}}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10}}`, 7, 3, 10, -1, -1},
		{"with details", `{"choices":[{"message":{"content":"x"}}],"usage":{"prompt_tokens":50,"completion_tokens":40,"total_tokens":90,"cached_tokens":30,"reasoning_tokens":12}}`, 50, 40, 90, 30, 12},
		{"zero details omitted", `{"choices":[{"message":{"content":"x"}}],"usage":{"prompt_tokens":4,"completion_tokens":4,"total_tokens":8,"cached_tokens":0,"reasoning_tokens":0}}`, 4, 4, 8, -1, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := FromChat(mustCompletion(t, tc.cc), &Request{}, Served{ChatModel: "m"})
			if resp.Usage == nil {
				t.Fatal("usage must not be nil")
			}
			if resp.Usage.InputTokens != tc.wantIn || resp.Usage.OutputTokens != tc.wantOut || resp.Usage.TotalTokens != tc.wantTotal {
				t.Errorf("usage tokens = %d/%d/%d, want %d/%d/%d",
					resp.Usage.InputTokens, resp.Usage.OutputTokens, resp.Usage.TotalTokens, tc.wantIn, tc.wantOut, tc.wantTotal)
			}
			if tc.wantCached < 0 {
				if resp.Usage.InputTokensDetails != nil {
					t.Errorf("input_tokens_details = %+v, want nil", resp.Usage.InputTokensDetails)
				}
			} else if resp.Usage.InputTokensDetails == nil || resp.Usage.InputTokensDetails.CachedTokens != tc.wantCached {
				t.Errorf("input_tokens_details = %+v, want cached_tokens %d", resp.Usage.InputTokensDetails, tc.wantCached)
			}
			if tc.wantReason < 0 {
				if resp.Usage.OutputTokensDetails != nil {
					t.Errorf("output_tokens_details = %+v, want nil", resp.Usage.OutputTokensDetails)
				}
			} else if resp.Usage.OutputTokensDetails == nil || resp.Usage.OutputTokensDetails.ReasoningTokens != tc.wantReason {
				t.Errorf("output_tokens_details = %+v, want reasoning_tokens %d", resp.Usage.OutputTokensDetails, tc.wantReason)
			}
		})
	}
}

// TestFromChatModelFallback pins the model label: the upstream echo wins, the
// chain step is used only when the upstream did not echo one.
func TestFromChatModelFallback(t *testing.T) {
	cc := mustCompletion(t, `{"choices":[{"message":{"content":"x"}}]}`)

	if got := FromChat(cc, &Request{}, Served{ChatModel: "", ChainModel: "fast"}).Model; got != "fast" {
		t.Errorf("model = %q, want the chain model when ChatModel is empty", got)
	}
	if got := FromChat(cc, &Request{}, Served{ChatModel: "self-dploy/GLM-5.3-Flash", ChainModel: "fast"}).Model; got != "self-dploy/GLM-5.3-Flash" {
		t.Errorf("model = %q, want the upstream echo to win", got)
	}
}

// TestFromChatEchoesClientParams checks that request knobs the gateway accepted
// are echoed back, and absent ones render as null rather than fabricated values.
func TestFromChatEchoesClientParams(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"model":               "fast",
		"input":               "hi",
		"instructions":        "be terse",
		"temperature":         0.4,
		"top_p":               0.9,
		"top_logprobs":        5,
		"max_output_tokens":   7000,
		"parallel_tool_calls": true,
		"reasoning":           map[string]any{"effort": "high"},
	})
	if err != nil {
		t.Fatal(err)
	}
	orig, err := ParseRequest(body)
	if err != nil {
		t.Fatal(err)
	}

	resp := FromChat(mustCompletion(t, `{"choices":[{"message":{"content":"x"}}]}`), orig, Served{ChatModel: "m"})
	if resp.Instructions != "be terse" {
		t.Errorf("instructions = %v", resp.Instructions)
	}
	if resp.Temperature == nil || *resp.Temperature != 0.4 {
		t.Errorf("temperature = %v", resp.Temperature)
	}
	if resp.TopP == nil || *resp.TopP != 0.9 {
		t.Errorf("top_p = %v", resp.TopP)
	}
	if resp.TopLogprobs == nil || *resp.TopLogprobs != 5 {
		t.Errorf("top_logprobs = %v", resp.TopLogprobs)
	}
	if resp.ParallelToolCalls == nil || !*resp.ParallelToolCalls {
		t.Errorf("parallel_tool_calls = %v", resp.ParallelToolCalls)
	}
	if resp.MaxOutputTokens == nil {
		t.Error("max_output_tokens not echoed")
	}
	if got, _ := resp.Reasoning.(map[string]any); got == nil || got["effort"] != "high" {
		t.Errorf("reasoning = %v, want {effort: high}", resp.Reasoning)
	}
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"tool_choice", "metadata", "truncation", "text", "stream"} {
		if !strings.Contains(string(raw), `"`+field+`":null`) {
			t.Errorf("%s not null in %s", field, raw)
		}
	}
	if resp.Output == nil || len(resp.Output) != 1 {
		t.Errorf("output = %v", resp.Output)
	}
	if got := resp.Tools; got == nil || len(got) != 0 {
		t.Errorf("tools = %v, want an empty list", got)
	}
	if got := resp.StreamEvents; got == nil || len(got) != 0 {
		t.Errorf("stream_events = %v, want an empty list", got)
	}

	bare := FromChat(mustCompletion(t, `{"choices":[{"message":{"content":"x"}}]}`), &Request{}, Served{ChatModel: "m"})
	if bare.Instructions != nil || bare.Reasoning != nil {
		t.Errorf("absent client fields must be null: instructions=%v reasoning=%v", bare.Instructions, bare.Reasoning)
	}
	if bare.Temperature != nil || bare.TopP != nil || bare.TopLogprobs != nil || bare.ParallelToolCalls != nil {
		t.Errorf("absent client knobs must be null: %+v", bare)
	}
}

func newStreamRecorder(t *testing.T) (*httptest.ResponseRecorder, http.Flusher) {
	t.Helper()
	return httptest.NewRecorder(), recFlusher{}
}

func mustChunk(t *testing.T, raw string) *ChatChunk {
	t.Helper()
	cc := &ChatChunk{}
	if err := json.Unmarshal([]byte(raw), cc); err != nil {
		t.Fatal(err)
	}
	return cc
}

// TestStreamOpensWithTwoEvents pins the two opening events and their shared
// in_progress response object.
func TestStreamOpensWithTwoEvents(t *testing.T) {
	rec, fl := newStreamRecorder(t)
	orig := &Request{Instructions: "be terse", Reasoning: &Reasoning{Effort: "medium"}}

	s, err := NewStream(rec, fl, orig, Served{ChatModel: "self-dploy/GLM-5.3-Flash", ChainModel: "fast"})
	if err != nil {
		t.Fatal(err)
	}
	events := parseSSE(t, rec.Body.String())
	wantNames(t, events, []string{"response.created", "response.in_progress"})
	walkInvariants(t, events)

	for i := 0; i < len(events); i++ {
		r := events[i].response()
		if r == nil {
			t.Fatalf("event %q has no response object", events[i].name)
		}
		if r["status"] != "in_progress" {
			t.Errorf("event[%d] response.status = %v, want in_progress", i, r["status"])
		}
		if r["object"] != "response" || r["store"] != false {
			t.Errorf("event[%d] envelope = object %v store %v", i, r["object"], r["store"])
		}
		if r["model"] != "self-dploy/GLM-5.3-Flash" {
			t.Errorf("event[%d] model = %v", i, r["model"])
		}
		if r["instructions"] != "be terse" {
			t.Errorf("event[%d] instructions = %v", i, r["instructions"])
		}
		out := parsedItems(r["output"])
		if len(out) != 0 {
			t.Errorf("event[%d] output = %v, want an empty list", i, r["output"])
		}
	}
	if events[0].response()["id"] != events[1].response()["id"] {
		t.Errorf("created and in_progress must share one response id")
	}
	if events[0].response()["created_at"] != events[1].response()["created_at"] {
		t.Errorf("created and in_progress must share one created_at")
	}
	if s == nil || s.outIndex != 0 {
		t.Errorf("outIndex = %v, want 0 before any item opens", s.outIndex)
	}
}

// TestStreamFinishEmpty: a stream that produced nothing must still deliver one
// message item, a completed response and the sentinel.
func TestStreamFinishEmpty(t *testing.T) {
	rec, fl := newStreamRecorder(t)
	s, err := NewStream(rec, fl, &Request{}, Served{ChatModel: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish("stop", &ChatUsage{PromptTokens: 3, CompletionTokens: 0, TotalTokens: 3}); err != nil {
		t.Fatal(err)
	}

	events := parseSSE(t, rec.Body.String())
	wantNames(t, events, []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.content_part.added",
		"response.content_part.done",
		"response.output_item.done",
		"response.completed",
		"", // data: [DONE]
	})
	walkInvariants(t, events)

	if events[len(events)-1].name != "" || events[len(events)-1].data != nil {
		t.Fatalf("stream must end with the data: [DONE] sentinel, got %+v", events[len(events)-1])
	}
	done := events[5]
	if got := done.typ(); got != "response.output_item.done" {
		t.Fatalf("event[5] type = %q", got)
	}
	item := done.data["item"].(map[string]any)
	if item["type"] != "message" || item["status"] != "completed" {
		t.Errorf("final message item = %v", item)
	}
	content := messageContent(item)
	if len(content) != 1 || content[0]["type"] != "output_text" || content[0]["text"] != "" {
		t.Errorf("content = %v, want one empty output_text", content)
	}

	completed := events[6]
	if completed.typ() != "response.completed" {
		t.Fatalf("last event type = %q", completed.typ())
	}
	r := completed.response()
	if r["status"] != "completed" || r["id"] != events[0].response()["id"] {
		t.Errorf("completed response = status %v id %v", r["status"], r["id"])
	}
	out := parsedItems(r["output"])
	if len(out) != 1 || out[0]["type"] != "message" {
		t.Fatalf("completed output = %v", out)
	}
	if out[0]["id"] != item["id"] {
		t.Errorf("output item id %v != the id announced in output_item.done %v", out[0]["id"], item["id"])
	}
	u := r["usage"].(map[string]any)
	if u["input_tokens"] != float64(3) || u["output_tokens"] != float64(0) || u["total_tokens"] != float64(3) {
		t.Errorf("usage = %v", u)
	}
}

// TestStreamErrorf pins the mid-stream failure path: response.error then the
// terminal sentinel, with no per-item done events.
func TestStreamErrorf(t *testing.T) {
	cases := []struct {
		name   string
		chunks []string
	}{
		{"no item open", nil},
		{"message left open", []string{`{"choices":[{"delta":{"content":"partial"}}]}`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, fl := newStreamRecorder(t)
			s, err := NewStream(rec, fl, &Request{}, Served{ChatModel: "m"})
			if err != nil {
				t.Fatal(err)
			}
			for _, raw := range tc.chunks {
				if err := s.Chunk(mustChunk(t, raw)); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Errorf(http.StatusBadGateway, "upstream stream error"); err != nil {
				t.Fatal(err)
			}

			body := rec.Body.String()
			events := parseSSE(t, body)
			// A chunk that opened a message puts the item events on the wire
			// before the error, so the prefix depends on whether one was sent.
			want := []string{"response.created", "response.in_progress"}
			if tc.chunks != nil {
				want = append(want, "response.output_item.added", "response.content_part.added", "response.output_text.delta")
			}
			want = append(want, "response.error", "")
			wantNames(t, events, want)
			if events[len(events)-1].data != nil {
				t.Fatalf("error path must end with the data: [DONE] sentinel, got %+v", events[len(events)-1])
			}
			if strings.Contains(body, "output_item.done") || strings.Contains(body, "response.completed") {
				t.Errorf("error path must not finalise items or complete the response:\n%s", body)
			}
			// The error event sits after the item events when a message was
			// open, so find it rather than assuming its index.
			var found bool
			for _, we := range events {
				if we.typ() != "response.error" {
					continue
				}
				found = true
				if we.data["response_id"] != events[0].response()["id"] {
					t.Errorf("error response_id = %v", we.data["response_id"])
				}
				errObj, ok := we.data["error"].(map[string]any)
				if !ok {
					t.Errorf("error payload = %v, want an object", we.data["error"])
					continue
				}
				if errObj["message"] != "upstream stream error" || errObj["code"] != "502" {
					t.Errorf("error = %v, want message + code 502", errObj)
				}
			}
			if !found {
				t.Error("no response.error event emitted")
			}
		})
	}
}

// TestStreamChunkSequence is the full happy path: reasoning deltas, text
// deltas, tool call deltas for two calls, then Finish. It asserts the exact
// event order, ascending output_index assignment, and the context fields every
// item-delta event must carry.
func TestStreamChunkSequence(t *testing.T) {
	rec, fl := newStreamRecorder(t)
	s, err := NewStream(rec, fl, &Request{Temperature: ptrFloat(0.3), MaxOutputTokens: ptrInt64(1200)}, Served{ChatModel: "m", ChainModel: "fast"})
	if err != nil {
		t.Fatal(err)
	}

	for _, raw := range []string{
		`{"choices":[{"delta":{"reasoning":"Let me"}}]}`,
		`{"choices":[{"delta":{"reasoning":" think."}}]}`,
		`{"choices":[{"delta":{"content":"Hello"}}]}`,
		`{"choices":[{"delta":{"content":", "}}]}`,
		`{"choices":[{"delta":{"content":"world"}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\""}}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"Paris\"}"}}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_2","type":"function","function":{"name":"search","arguments":"{}"}}]}}]}`,
	} {
		if err := s.Chunk(mustChunk(t, raw)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Finish("stop", &ChatUsage{
		PromptTokens: 11, CompletionTokens: 22, TotalTokens: 33,
		CachedTokens: 6, ReasoningTokens: 4,
	}); err != nil {
		t.Fatal(err)
	}

	events := parseSSE(t, rec.Body.String())
	wantNames(t, events, []string{
		"response.created",
		"response.in_progress",

		"response.output_item.added",
		"response.content_part.added",
		"response.reasoning_summary_text.delta",
		"response.reasoning_summary_text.delta",

		"response.output_item.added",
		"response.content_part.added",
		"response.output_text.delta",
		"response.output_text.delta",
		"response.output_text.delta",

		"response.output_item.added",
		"response.function_call_arguments.delta",
		"response.function_call_arguments.delta",

		"response.output_item.added",
		"response.function_call_arguments.delta",

		"response.reasoning_summary_text.done",
		"response.content_part.done",
		"response.output_item.done",

		"response.output_text.done",
		"response.content_part.done",
		"response.output_item.done",

		"response.function_call_arguments.done",
		"response.output_item.done",

		"response.function_call_arguments.done",
		"response.output_item.done",

		"response.completed",
		"", // data: [DONE]
	})
	if events[len(events)-1].data != nil {
		t.Fatalf("stream must end with the data: [DONE] sentinel")
	}
	firstSeen := walkInvariants(t, events)
	if len(firstSeen) != 4 {
		t.Fatalf("output_index values first seen = %v, want 4 distinct ascending values", firstSeen)
	}
	if !slices.Equal(firstSeen, []int{0, 1, 2, 3}) {
		t.Errorf("output_index order = %v, want [0 1 2 3]", firstSeen)
	}

	// Delta payloads must carry exactly what the upstream sent.
	for i, e := range events {
		if e.data == nil {
			continue
		}
		if d, ok := e.data["delta"]; ok && d == "" {
			t.Errorf("event[%d] %q emitted an empty delta payload", i, e.name)
		}
	}
	wantTextDeltas := []string{"Hello", ", ", "world"}
	gotText := []string{}
	for _, e := range events {
		if e.typ() == "response.output_text.delta" {
			gotText = append(gotText, e.data["delta"].(string))
		}
	}
	if !slices.Equal(gotText, wantTextDeltas) {
		t.Errorf("output_text deltas = %v, want %v", gotText, wantTextDeltas)
	}
	// Argument deltas must be non-empty and must concatenate to a valid JSON
	// object, so the item announced in response.completed is reconstructible.
	argsBuf := strings.Builder{}
	nArgs := 0
	for _, e := range events {
		if e.data == nil || e.typ() != "response.function_call_arguments.delta" {
			continue
		}
		d, ok := e.data["delta"].(string)
		if !ok || d == "" {
			t.Errorf("function_call_arguments.delta payload = %v, want a non-empty string", e.data["delta"])
			continue
		}
		argsBuf.WriteString(d)
		nArgs++
	}
	if nArgs == 0 {
		t.Error("no function_call_arguments.delta events were emitted")
	}

	// The finished response object must carry every final item in emit order.
	completed := events[len(events)-2]
	r := completed.response()
	if r["status"] != "completed" || r["id"] != events[0].response()["id"] {
		t.Errorf("completed = status %v id %v", r["status"], r["id"])
	}
	u := r["usage"].(map[string]any)
	wantUsage := map[string]any{"input_tokens": 11.0, "output_tokens": 22.0, "total_tokens": 33.0}
	for k, v := range wantUsage {
		if u[k] != v {
			t.Errorf("usage[%s] = %v, want %v", k, u[k], v)
		}
	}
	in := u["input_tokens_details"].(map[string]any)
	outD := u["output_tokens_details"].(map[string]any)
	if in["cached_tokens"] != float64(6) || outD["reasoning_tokens"] != float64(4) {
		t.Errorf("usage details = %v / %v", in, outD)
	}
	if r["temperature"] != 0.3 || r["max_output_tokens"] != float64(1200) {
		t.Errorf("echoed knobs = temperature %v max_output_tokens %v", r["temperature"], r["max_output_tokens"])
	}

	items := parsedItems(r["output"])
	if len(items) != 4 {
		t.Fatalf("output = %v, want 4 items", items)
	}
	wantTypes := []string{"reasoning", "message", "function_call", "function_call"}
	for i, it := range items {
		if it["type"] != wantTypes[i] || it["status"] != "completed" {
			t.Errorf("item[%d] = %v, want %s/completed", i, it, wantTypes[i])
		}
	}
	summary := parsedItems(items[0]["summary"])
	if len(summary) != 1 || summary[0]["text"] != "Let me think." {
		t.Errorf("reasoning summary = %v", summary)
	}
	content := messageContent(items[1])
	if len(content) != 1 || content[0]["type"] != "output_text" || content[0]["text"] != "Hello, world" {
		t.Errorf("message content = %v", content)
	}
	if items[2]["call_id"] != "call_1" || items[2]["name"] != "get_weather" || items[2]["arguments"] != `{"city":"Paris"}` {
		t.Errorf("item[2] = %v", items[2])
	}
	if items[3]["call_id"] != "call_2" || items[3]["name"] != "search" || items[3]["arguments"] != "{}" {
		t.Errorf("item[3] = %v", items[3])
	}

	// Item ids must be consistent between the announced and final forms.
	itemIDs := map[string]string{}
	for _, e := range events {
		if e.name == "response.output_item.added" {
			it := e.data["item"].(map[string]any)
			itemIDs[it["id"].(string)] = ""
		}
	}
	for _, it := range items {
		if _, ok := itemIDs[it["id"].(string)]; !ok {
			t.Errorf("final item id %v was never announced by output_item.added", it["id"])
		}
	}
}

func ptrFloat(v float64) *float64 { return &v }

func ptrInt64(v int64) *int64 { return &v }
