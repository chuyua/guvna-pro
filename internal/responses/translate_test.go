package responses

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// chatBody parses a Responses request the way the server does, translates it,
// and returns the chat body as raw bytes (for byte-level assertions) and as a
// map (for field-level ones).
func chatBody(t *testing.T, raw string) ([]byte, map[string]any) {
	t.Helper()
	req, err := ParseRequest([]byte(raw))
	if err != nil {
		t.Fatalf("ParseRequest: %v", err)
	}
	chat, err := ToChat(req)
	if err != nil {
		t.Fatalf("ToChat: %v", err)
	}
	body, err := chat.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("marshalled body is not an object: %v\n%s", err, body)
	}
	return body, m
}

func chatMsgs(t *testing.T, body map[string]any) []map[string]any {
	t.Helper()
	raw, ok := body["messages"].([]any)
	if !ok {
		t.Fatalf("messages is %v (type %T), want an array", body["messages"], body["messages"])
	}
	out := make([]map[string]any, 0, len(raw))
	for i, m := range raw {
		out = append(out, chatObj(t, m, fmt.Sprintf("message %d", i)))
	}
	return out
}

// chatObj is a value that must be a JSON object, or the test fails.
func chatObj(t *testing.T, v any, what string) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("%s is %v (type %T), want an object", what, v, v)
	}
	return m
}

// chatList is a value that must be a JSON array.
func chatList(t *testing.T, v any, what string) []any {
	t.Helper()
	l, ok := v.([]any)
	if !ok {
		t.Fatalf("%s is %v (type %T), want an array", what, v, v)
	}
	return l
}

// chatAbsent asserts that none of the fields reached the chat body.
func chatAbsent(t *testing.T, body map[string]any, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if _, has := body[k]; has {
			t.Errorf("body must not contain %q", k)
		}
	}
}

func TestToChatStringInputBecomesSingleUserMessage(t *testing.T) {
	body, m := chatBody(t, `{"model":"gpt-5.5","input":"hello world"}`)
	got := chatMsgs(t, m)
	if len(got) != 1 {
		t.Fatalf("messages = %d, want 1\n%s", len(got), body)
	}
	if got[0]["role"] != "user" || got[0]["content"] != "hello world" {
		t.Fatalf("message = %v, want role=user content=\"hello world\"", got[0])
	}
	if m["model"] != "gpt-5.5" {
		t.Errorf("model = %v, want gpt-5.5", m["model"])
	}
	chatAbsent(t, m, "instructions", "tools", "tool_choice", "stream_options")
}

func TestToChatEmptyRequestMarshalsCleanly(t *testing.T) {
	body, _ := chatBody(t, `{"model":"m"}`)
	want := `{"messages":[],"model":"m","stream":false}`
	if string(body) != want {
		t.Fatalf("body = %s,\nwant %s", body, want)
	}
}

func TestToChatMessageItemsWithTextAndImageParts(t *testing.T) {
	body, m := chatBody(t, `{
		"model": "m",
		"input": [
			{"type": "message", "role": "system", "content": "Answer in one line."},
			{"type": "message", "role": "developer", "content": "Use the shell tool when asked."},
			{"type": "message", "role": "user", "content": [
				{"type": "input_text", "text": "What animal is in"},
				{"type": "input_image", "image_url": "https://x.test/cat.png", "detail": "high"},
				{"type": "input_text", "text": " this picture?"}
			]},
			{"type": "message", "role": "assistant", "content": [
				{"type": "output_text", "text": "It looks like a"}
			]},
			{"type": "message", "role": "user", "content": [
				{"type": "input_file", "file_id": "file_abc"},
				{"type": "refusal", "refusal": "not answering"}
			]}
		]
	}`)
	got := chatMsgs(t, m)
	if len(got) != 5 {
		t.Fatalf("messages = %d, want 5\n%s", len(got), body)
	}
	if got[0]["role"] != "system" || got[0]["content"] != "Answer in one line." {
		t.Errorf("msg0 = %v, want system/\"Answer in one line.\"", got[0])
	}
	// developer has no chat role of its own.
	if got[1]["role"] != "system" || got[1]["content"] != "Use the shell tool when asked." {
		t.Errorf("msg1 = %v, want developer mapped to system", got[1])
	}

	// A multimodal message keeps a parts array, in the order sent.
	parts := chatList(t, got[2]["content"], "msg2 content")
	if len(parts) != 3 {
		t.Fatalf("msg2 parts = %d, want 3: %v", len(parts), parts)
	}
	if p := chatObj(t, parts[0], "part0"); p["type"] != "text" || p["text"] != "What animal is in" {
		t.Errorf("part0 = %v", p)
	}
	p1 := chatObj(t, parts[1], "part1")
	if p1["type"] != "image_url" {
		t.Fatalf("part1 = %v, want type=image_url", p1)
	}
	if img := chatObj(t, p1["image_url"], "image_url"); img["url"] != "https://x.test/cat.png" || img["detail"] != "high" {
		t.Errorf("image_url = %v, want url and detail passed through", img)
	}
	if p := chatObj(t, parts[2], "part2"); p["type"] != "text" || p["text"] != " this picture?" {
		t.Errorf("part2 = %v", p)
	}

	// Text-only content collapses back to a string.
	if got[3]["role"] != "assistant" || got[3]["content"] != "It looks like a" {
		t.Errorf("msg3 = %v, want assistant/\"It looks like a\"", got[3])
	}

	// Every part dropped: the message still arrives with empty content, so the
	// turn order an upstream enforces is unchanged.
	if got[4]["role"] != "user" || got[4]["content"] != "" {
		t.Errorf("msg4 = %v, want user with content \"\"", got[4])
	}
}

func TestToChatCodexTranscriptRoundTripsArguments(t *testing.T) {
	const wantArgs = `{"cmd":"ls -la /tmp","timeout":30}`
	const callID = "call_9f3a2b"
	body, m := chatBody(t, `{
		"model": "gpt-5.5-codex",
		"instructions": "You are a coding agent. Confirm before you edit.",
		"stream": true,
		"input": [
			{"type": "message", "role": "user", "content": "List /tmp."},
			{"type": "function_call", "name": "shell", "call_id": "`+callID+`", "id": "fc_1",
			 "arguments": "{\"cmd\":\"ls -la /tmp\",\"timeout\":30}"},
			{"type": "function_call_output", "call_id": "`+callID+`", "output": "total 0"}
		]
	}`)
	got := chatMsgs(t, m)
	if len(got) != 4 {
		t.Fatalf("messages = %d, want 4\n%s", len(got), body)
	}
	wantRoles := []string{"system", "user", "assistant", "tool"}
	for i, r := range wantRoles {
		if got[i]["role"] != r {
			t.Errorf("msg%d role = %v, want %s", i, got[i]["role"], r)
		}
	}
	if got[0]["content"] != "You are a coding agent. Confirm before you edit." {
		t.Errorf("msg0 = %v, want the instructions as a system message", got[0])
	}
	if got[1]["content"] != "List /tmp." {
		t.Errorf("msg1 content = %v", got[1]["content"])
	}

	// The assistant message carries the call; content stays empty.
	if got[2]["content"] != "" {
		t.Errorf("msg2 content = %v, want \"\"", got[2]["content"])
	}
	calls := chatList(t, got[2]["tool_calls"], "msg2 tool_calls")
	if len(calls) != 1 {
		t.Fatalf("tool_calls = %d, want 1", len(calls))
	}
	call := chatObj(t, calls[0], "call0")
	if call["id"] != callID || call["type"] != "function" {
		t.Errorf("call0 = %v, want id=%s type=function", call, callID)
	}
	fn := chatObj(t, call["function"], "call0.function")
	if fn["name"] != "shell" {
		t.Errorf("call0.function.name = %v, want shell", fn["name"])
	}
	if fn["arguments"] != wantArgs {
		t.Errorf("call0.function.arguments = %v, want %v", fn["arguments"], wantArgs)
	}
	// The argument text is what a client diffs to decide what to run, so it
	// must survive the round trip byte-for-byte, escaping included.
	wire, err := json.Marshal(wantArgs)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), string(wire)) {
		t.Errorf("arguments not byte-identical in the marshalled body:\n%s", body)
	}

	if got[3]["tool_call_id"] != callID || got[3]["content"] != "total 0" {
		t.Errorf("msg3 = %v, want tool_call_id=%s content=\"total 0\"", got[3], callID)
	}
}

func TestToChatCustomToolCallMarshalsObjectInput(t *testing.T) {
	const wantArgs = `{"edits":[{"new":"func a(int)","old":"func a()"}],"path":"internal/x.go"}`
	body, m := chatBody(t, `{
		"model": "m",
		"input": [
			{"type": "custom_tool_call", "name": "apply_patch", "call_id": "call_c1",
			 "input": {"path": "internal/x.go", "edits": [{"old": "func a()", "new": "func a(int)"}]}},
			{"type": "custom_tool_call_output", "call_id": "call_c1", "output": "ok"}
		]
	}`)
	got := chatMsgs(t, m)
	if len(got) != 2 {
		t.Fatalf("messages = %d, want 2\n%s", len(got), body)
	}
	call := chatObj(t, chatList(t, got[0]["tool_calls"], "tool_calls")[0], "call0")
	fn := chatObj(t, call["function"], "call0.function")
	if fn["name"] != "apply_patch" || call["id"] != "call_c1" {
		t.Errorf("call0 = %v", call)
	}
	// custom_tool_call carries a JSON object where chat wants a string, so it
	// must be marshalled, not forwarded as an object.
	args, isStr := fn["arguments"].(string)
	if !isStr {
		t.Fatalf("arguments is %T, want string", fn["arguments"])
	}
	if args != wantArgs {
		t.Errorf("arguments = %s,\nwant %s", args, wantArgs)
	}
	if got[1]["role"] != "tool" || got[1]["tool_call_id"] != "call_c1" || got[1]["content"] != "ok" {
		t.Errorf("msg1 = %v, want tool/call_c1/ok", got[1])
	}
}

func TestToChatMergesConsecutiveCallsIntoOneAssistantMessage(t *testing.T) {
	// Codex commonly fires two tools in one turn. Chat requires every tool
	// message to sit directly after the assistant message that declares its
	// call, so the calls must share one message.
	body, m := chatBody(t, `{
		"model": "m",
		"input": [
			{"type": "message", "role": "user", "content": "Do both."},
			{"type": "function_call", "name": "shell", "call_id": "call_1", "arguments": "{\"cmd\":\"pwd\"}"},
			{"type": "custom_tool_call", "name": "apply_patch", "call_id": "call_2", "input": {"path": "a.go"}},
			{"type": "function_call_output", "call_id": "call_2", "output": "patched"},
			{"type": "function_call_output", "call_id": "call_1", "output": "/repo"},
			{"type": "message", "role": "user", "content": "And that is the end."}
		]
	}`)
	got := chatMsgs(t, m)
	if len(got) != 5 {
		t.Fatalf("messages = %d, want 5\n%s", len(got), body)
	}
	wantRoles := []string{"user", "assistant", "tool", "tool", "user"}
	for i, r := range wantRoles {
		if got[i]["role"] != r {
			t.Errorf("msg%d role = %v, want %s", i, got[i]["role"], r)
		}
	}
	calls := chatList(t, got[1]["tool_calls"], "msg1 tool_calls")
	if len(calls) != 2 {
		t.Fatalf("tool_calls = %d, want both calls merged into one message", len(calls))
	}
	ids := []string{}
	for i, c := range calls {
		co := chatObj(t, c, fmt.Sprintf("call %d", i))
		ids = append(ids, strVal(co["id"]))
		if co["type"] != "function" {
			t.Errorf("call %d type = %v, want function", i, co["type"])
		}
	}
	if ids[0] != "call_1" || ids[1] != "call_2" {
		t.Errorf("call order = %v, want [call_1 call_2]", ids)
	}
	// Both tool outputs follow the assistant message, in the order sent.
	if got[2]["tool_call_id"] != "call_2" || got[2]["content"] != "patched" {
		t.Errorf("msg2 = %v", got[2])
	}
}

func strVal(v any) string {
	s, _ := v.(string)
	return s
}

func TestToChatKeepsOnlyNamedTools(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tools string
		want  []string
	}{
		{
			name: "named tools survive",
			tools: `[
				{"type": "function", "name": "get_weather", "description": "Weather for a city",
				 "parameters": {"type": "object", "properties": {"city": {"type": "string"}},
					 "required": ["city"], "additionalProperties": false}},
				{"type": "custom", "name": "patch", "description": "Edit a file", "parameters": {}},
				{"type": "shell", "name": "shell", "description": "Run a command",
				 "parameters": {"type": "object", "properties": {"cmd": {"type": "string"}},
					 "required": ["cmd"], "additionalProperties": false}},
				{"type": "web_search"},
				{"type": "file_search"},
				{"type": "function", "name": "no_desc", "parameters": null},
				{"strict": true, "name": "no_type", "description": "schema flag must not leak"}
			]`,
			want: []string{"get_weather", "patch", "shell", "no_desc", "no_type"},
		},
		{
			name:  "all droppable",
			tools: `[{"type": "web_search"}, {"type": "file_search"}, {"type": "image_generation"}]`,
			want:  nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, m := chatBody(t, `{"model":"m","input":"hi","tools":`+tc.tools+`}`)
			tools, ok := m["tools"].([]any)
			if !ok {
				if tc.want == nil {
					return // an empty filter must drop the key, not emit []
				}
				t.Fatalf("tools missing from body: %s", body)
			}
			if len(tools) != len(tc.want) {
				t.Fatalf("tools = %d, want %d\n%s", len(tools), len(tc.want), body)
			}
			for i, w := range tc.want {
				fn := chatObj(t, chatObj(t, tools[i], fmt.Sprintf("tool %d", i))["function"], fmt.Sprintf("tool %d function", i))
				if fn["name"] != w {
					t.Errorf("tool %d name = %v, want %q", i, fn["name"], w)
				}
			}
			// Codex marks schemas strict; a chat upstream that rejects the
			// field must never see it.
			if strings.Contains(string(body), "strict") {
				t.Errorf("body must not emit \"strict\": %s", body)
			}
			if tc.name != "named tools survive" {
				return
			}
			fn0 := chatObj(t, chatObj(t, tools[0], "tool0")["function"], "tool0.function")
			if params := chatObj(t, fn0["parameters"], "tool0 parameters"); !isList(params["required"], "city") {
				t.Errorf("tool0 parameters lost its schema: %v", fn0["parameters"])
			}
			fn1 := chatObj(t, chatObj(t, tools[1], "tool1")["function"], "tool1.function")
			if _, has := fn1["parameters"]; has {
				t.Errorf("tool1 must not emit an empty parameters object: %v", tools[1])
			}
		})
	}
}

func isList(v any, val string) bool {
	l, ok := v.([]any)
	return ok && len(l) == 1 && l[0] == val
}

func TestToChatToolChoice(t *testing.T) {
	named := func(name string) any {
		return map[string]any{"type": "function", "function": map[string]any{"name": name}}
	}
	for _, tc := range []struct {
		name   string
		choice string
		want   any // nil means the field must be omitted
	}{
		{"auto", `"auto"`, "auto"},
		{"none", `"none"`, "none"},
		{"required", `"required"`, "required"},
		{"named function", `{"type": "function", "name": "shell"}`, named("shell")},
		{"allowed_tools takes the first named tool", `{"type": "allowed_tools", "tools": [{"type": "shell", "name": "shell"}, {"type": "function", "name": "other"}]}`, named("shell")},
		{"garbage string", `"allowlist"`, nil},
		{"nameless object", `{"type": "custom"}`, nil},
		{"null", `null`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, m := chatBody(t, `{"model":"m","input":"hi","tool_choice":`+tc.choice+`}`)
			got, ok := m["tool_choice"]
			if !ok {
				if tc.want == nil {
					return
				}
				t.Fatalf("tool_choice missing:\n%s", body)
			}
			if tc.want == nil {
				t.Fatalf("tool_choice must be omitted, got %v\n%s", got, body)
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("tool_choice = %v, want %v\n%s", got, tc.want, body)
			}
		})
	}
}

func TestToChatParameterMapping(t *testing.T) {
	_, m := chatBody(t, `{
		"model": "m",
		"input": "hi",
		"temperature": 0.2,
		"top_p": 0.9,
		"max_output_tokens": 8192,
		"parallel_tool_calls": true,
		"user": "user-42",
		"reasoning": {"effort": "xhigh", "summary": "auto"},
		"store": false,
		"previous_response_id": "resp_1",
		"max_input_tokens": 16000,
		"metadata": {"trace": "abc"},
		"top_logprobs": 5,
		"logprobs": true,
		"truncation": "auto",
		"text": {"verbosity": "low"}
	}`)
	if m["max_tokens"] != float64(8192) {
		t.Errorf("max_tokens = %v, want 8192", m["max_tokens"])
	}
	if m["temperature"] != 0.2 {
		t.Errorf("temperature = %v, want 0.2", m["temperature"])
	}
	if m["top_p"] != 0.9 {
		t.Errorf("top_p = %v, want 0.9", m["top_p"])
	}
	if m["parallel_tool_calls"] != true {
		t.Errorf("parallel_tool_calls = %v, want true", m["parallel_tool_calls"])
	}
	if m["user"] != "user-42" {
		t.Errorf("user = %v, want user-42", m["user"])
	}
	// xhigh has no chat equivalent; capping to high is better than a 4xx.
	if m["reasoning_effort"] != "high" {
		t.Errorf("reasoning_effort = %v, want high", m["reasoning_effort"])
	}
	chatAbsent(t, m, "store", "previous_response_id", "max_input_tokens", "metadata",
		"top_logprobs", "logprobs", "truncation", "text", "reasoning", "input")
}

func TestToChatReasoningEffortMapping(t *testing.T) {
	for _, tc := range []struct {
		name   string
		effort string
		want   string // "" means the field must be omitted
	}{
		{"low", "low", "low"},
		{"medium", "medium", "medium"},
		{"high", "high", "high"},
		{"xhigh caps to high", "xhigh", "high"},
		{"minimal omitted", "minimal", ""},
		{"empty omitted", "", ""},
		{"unknown omitted", "deepthink", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, m := chatBody(t, `{"model":"m","input":"hi","reasoning":{"effort":"`+tc.effort+`"}}`)
			got, ok := m["reasoning_effort"]
			if tc.want == "" {
				if ok {
					t.Fatalf("reasoning_effort must be omitted for %q, got %v\n%s", tc.effort, got, body)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("reasoning_effort = %v, want %q\n%s", got, tc.want, body)
			}
		})
	}
}

func TestToChatStreamOptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		stream  bool
		wantOpt bool
	}{
		{"stream true", `{"model":"m","input":"hi","stream":true}`, true, true},
		{"stream false", `{"model":"m","input":"hi","stream":false}`, false, false},
		{"stream absent", `{"model":"m","input":"hi"}`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, m := chatBody(t, tc.body)
			if m["stream"] != tc.stream {
				t.Fatalf("stream = %v, want %v\n%s", m["stream"], tc.stream, body)
			}
			opt, ok := m["stream_options"]
			if !ok {
				if tc.wantOpt {
					t.Fatalf("stream_options missing:\n%s", body)
				}
				return
			}
			if !tc.wantOpt {
				t.Fatalf("stream_options must be absent:\n%s", body)
				return
			}
			if o := chatObj(t, opt, "stream_options"); o["include_usage"] != true {
				t.Errorf("stream_options.include_usage = %v, want true", o["include_usage"])
			}
		})
	}
}

func TestToChatDropsUnknownItemsWithoutBreakingOrder(t *testing.T) {
	body, m := chatBody(t, `{
		"model": "m",
		"input": [
			{"type": "message", "role": "user", "content": "first"},
			{"type": "reasoning", "id": "rs_1", "status": "completed",
			 "summary": [{"type": "summary_text", "text": "thinking out loud"}]},
			{"type": "item_reference", "id": "rs_1"},
			{"type": "web_search_call", "id": "ws_1", "action": {"query": "golang"}},
			{"type": "file_search_call", "id": "fs_1"},
			{"type": "computer_call", "id": "cm_1"},
			{"type": "mcp_call", "id": "mc_1"},
			{"type": "image_generation_call", "id": "ig_1"},
			{"type": "code_interpreter_call", "id": "ci_1"},
			{"type": "container_call", "id": "ct_1"},
			{"type": "image_call", "id": "ic_1"},
			{"type": "custom", "id": "cu_1"},
			{"type": "input_file", "file_id": "file_1"},
			{"type": "message", "role": "assistant", "content": [
				{"type": "refusal", "refusal": "not answering"},
				{"type": "input_file", "file_id": "file_2"}
			]},
			{"type": "message", "role": "user", "content": "second"},
			"a bare string is not an item",
			42
		]
	}`)
	got := chatMsgs(t, m)
	if len(got) != 3 {
		t.Fatalf("messages = %d, want 3 (only message items survive)\n%s", len(got), body)
	}
	want := []struct {
		role    string
		content string
	}{
		{"user", "first"},
		{"assistant", ""},
		{"user", "second"},
	}
	for i, w := range want {
		if got[i]["role"] != w.role || got[i]["content"] != w.content {
			t.Errorf("msg%d = %v, want role=%s content=%q", i, got[i], w.role, w.content)
		}
	}
}

func TestToChatMessagesWinsOverInput(t *testing.T) {
	body, m := chatBody(t, `{
		"model": "m",
		"instructions": "You are terse.",
		"messages": [
			{"role": "system", "content": "Never mention Python."},
			{"role": "user", "name": "alice", "content": [
				{"type": "text", "text": "hello"},
				{"type": "image_url", "image_url": {"url": "https://x.test/i.png"}}
			]}
		],
		"input": "THIS INPUT MUST BE IGNORED",
		"temperature": 1.5
	}`)
	got := chatMsgs(t, m)
	if len(got) != 3 {
		t.Fatalf("messages = %d, want 3 (instructions + 2 verbatim)\n%s", len(got), body)
	}
	if got[0]["role"] != "system" || got[0]["content"] != "You are terse." {
		t.Errorf("msg0 = %v", got[0])
	}
	if got[1]["role"] != "system" || got[1]["content"] != "Never mention Python." {
		t.Errorf("msg1 = %v", got[1])
	}
	// messages is the verbatim escape hatch: name and the content array pass
	// through untouched.
	if got[2]["name"] != "alice" {
		t.Errorf("msg2 name = %v, want alice", got[2]["name"])
	}
	if parts := chatList(t, got[2]["content"], "msg2 content"); len(parts) != 2 {
		t.Errorf("msg2 content parts = %d, want 2", len(parts))
	}
	if strings.Contains(string(body), "IGNORED") {
		t.Errorf("input leaked past messages: %s", body)
	}
	if m["temperature"] != 1.5 {
		t.Errorf("temperature = %v, want 1.5 (parameters are independent of input)", m["temperature"])
	}
}

func TestToChatNullMessagesFallsBackToInput(t *testing.T) {
	_, m := chatBody(t, `{"model":"m","messages":null,"input":"from input"}`)
	got := chatMsgs(t, m)
	if len(got) != 1 || got[0]["role"] != "user" || got[0]["content"] != "from input" {
		t.Fatalf("messages = %v, want one user message from input", got)
	}
}

func TestToChatRejectsMalformedInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"input is an object", `{"model":"m","input":{"a":1}}`},
		{"input is a number", `{"model":"m","input":7}`},
		{"input is a boolean", `{"model":"m","input":true}`},
		{"messages is not an array", `{"model":"m","messages":{"a":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := ParseRequest([]byte(tc.body))
			if err != nil {
				t.Fatalf("ParseRequest: %v", err)
			}
			if _, err := ToChat(req); err == nil {
				t.Fatalf("ToChat accepted %s", tc.body)
			}
		})
	}
	if _, err := ToChat(nil); err == nil {
		t.Fatal("ToChat(nil) must fail rather than panic")
	}
}
