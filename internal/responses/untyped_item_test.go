package responses

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestUntypedMessageItem pins the field Codex and most other clients omit: a
// Responses input item carries a role but no "type". The API defaults such an
// item to a message. Dropping it instead leaves the translated body with zero
// messages, and upstreams answer with 400 "Cannot apply chat template to an
// empty conversation" — a failure that stays invisible whenever instructions
// supplies a system message first.
func TestUntypedMessageItem(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantMin int
		wantHas []string
	}{
		{
			name:    "single untyped user message",
			input:   `[{"role":"user","content":[{"type":"input_text","text":"hi"}]}]`,
			wantMin: 1,
			wantHas: []string{`"role":"user"`},
		},
		{
			name:    "untyped message with tools must not empty the conversation",
			input:   `[{"role":"user","content":[{"type":"input_text","text":"list the files"}]}]`,
			wantMin: 1,
			wantHas: []string{`"role":"user"`},
		},
		{
			name:    "untyped developer role becomes system",
			input:   `[{"role":"developer","content":"be terse"}]`,
			wantMin: 1,
			wantHas: []string{`"role":"system"`},
		},
		{
			name: "typed and untyped items mix",
			input: `[{"type":"message","role":"system","content":"be terse"},` +
				`{"role":"user","content":"hi"}]`,
			wantMin: 2,
			wantHas: []string{`"role":"system"`, `"role":"user"`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chat, err := ToChat(&Request{
				Model: "m",
				Input: json.RawMessage(tc.input),
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(chat.Messages) < tc.wantMin {
				t.Fatalf("messages = %v (%d), want at least %d — an empty conversation is a hard 400 upstream",
					chat.Messages, len(chat.Messages), tc.wantMin)
			}
			body, err := chat.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			for _, needle := range tc.wantHas {
				if !jsonContains(body, needle) {
					t.Errorf("body missing %s: %s", needle, body)
				}
			}
		})
	}
}

// TestEmptyConversationIsImpossible guards the invariant the upstreams depend on:
// a request that has any user-visible input must translate to at least one chat
// message.
func TestEmptyConversationIsImpossible(t *testing.T) {
	inputs := []string{
		`"just a string"`,
		`[{"role":"user","content":"hi"}]`,
		`[{"role":"user","content":[{"type":"input_text","text":"hi"}]}]`,
		`[{"type":"message","role":"user","content":"hi"}]`,
	}
	for i, in := range inputs {
		chat, err := ToChat(&Request{Model: "m", Input: json.RawMessage(in)})
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if len(chat.Messages) == 0 {
			t.Errorf("case %d (%s) translated to zero messages", i, in)
		}
	}
}

// jsonContains reports whether needle appears in the marshalled body. json
// Marshal emits compact JSON, so a substring check is exact for flat keys.
func jsonContains(body []byte, needle string) bool {
	return strings.Contains(string(body), needle)
}
