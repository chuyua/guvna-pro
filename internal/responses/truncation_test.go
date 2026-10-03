package responses

import (
	"bufio"
	"encoding/json"
	"testing"
)

// TestFromChatTruncationIsIncomplete pins the status Codex relies on to tell a
// max-output cut-off from a natural stop. The Responses wire has no
// finish_reason field, so status plus incomplete_details are the only signal.
func TestFromChatTruncationIsIncomplete(t *testing.T) {
	cases := []struct {
		name        string
		finish      string
		wantStatus  string
		wantDetails bool
	}{
		{"stop", "stop", "completed", false},
		{"length", "length", "incomplete", true},
		{"content_filter", "content_filter", "incomplete", true},
		{"no choices at all", "", "completed", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cc := mustCompletion(t, `{"choices":[{"message":{"content":"x"},"finish_reason":"`+tc.finish+`"}]}`)
			resp := FromChat(cc, &Request{}, Served{ChatModel: "m"})
			if resp.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", resp.Status, tc.wantStatus)
			}
			if tc.wantDetails && resp.IncompleteDetails != nil {
				wantReason := "max_output_tokens"
				if tc.finish == "content_filter" {
					wantReason = "content_filter"
				}
				details, ok := resp.IncompleteDetails.(map[string]any)
				if !ok || details["reason"] != wantReason {
					t.Errorf("incomplete_details = %v, want reason %s", resp.IncompleteDetails, wantReason)
				}
			}
			if tc.wantDetails && resp.IncompleteDetails == nil {
				t.Error("incomplete_details = nil, want reason max_output_tokens")
			} else if !tc.wantDetails && resp.IncompleteDetails != nil {
				t.Errorf("incomplete_details = %v, want nil", resp.IncompleteDetails)
			}
		})
	}
}

// TestStreamTruncationIsIncomplete covers the streaming path: the terminal
// response.incomplete must carry the truncation status and reason.
func TestStreamTruncationIsIncomplete(t *testing.T) {
	rec, fl := newStreamRecorder(t)
	s, err := NewStream(rec, fl, &Request{}, Served{ChatModel: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Chunk(mustChunk(t, `{"choices":[{"delta":{"content":"Hel"}}]}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish("length", &ChatUsage{PromptTokens: 5, CompletionTokens: 10, TotalTokens: 15}); err != nil {
		t.Fatal(err)
	}

	var last map[string]any
	sc := bufio.NewScanner(rec.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if len(line) < 6 || line[:6] != "data: " {
			continue
		}
		if line[6:] == "[DONE]" {
			break
		}
		last = parseData(t, line[6:])
	}
	if last == nil {
		t.Fatal("no terminal event")
	}
	if last["type"] != "response.incomplete" {
		t.Fatalf("terminal type=%v, want response.incomplete", last["type"])
	}
	resp, ok := last["response"].(map[string]any)
	if !ok {
		t.Fatalf("terminal event = %v", last)
	}
	if resp["status"] != "incomplete" {
		t.Errorf("status = %v, want incomplete", resp["status"])
	}
	if resp["incomplete_details"] == nil {
		t.Error("incomplete_details = nil, want reason max_output_tokens")
	}
}

// parseData decodes one SSE data payload into a map.
func parseData(t *testing.T, payload string) map[string]any {
	t.Helper()
	out := map[string]any{}
	if err := json.Unmarshal([]byte(payload), &out); err != nil {
		t.Fatalf("payload %q is not a JSON object: %v", payload, err)
	}
	return out
}
