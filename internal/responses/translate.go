// translate.go maps a Responses request onto the chat completions body that
// every upstream accepts. render.go does the response edge; together they let
// the /v1/responses handler reuse the chat chain loop unchanged.
package responses

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ToChat translates a Responses request into a Chat Completions request body.
//
// The mapping drops rather than guesses. An upstream that sees a field it does
// not understand answers 4xx, which burns a chain step and on a cold chain
// every step, so every Responses feature without a chat equivalent is absent
// from the result rather than approximated. What does survive must survive
// exactly: a tool call's arguments are the string clients diff to decide what
// to run, so function_call.arguments passes through byte-for-byte.
//
// The error is reserved for input the client cannot have meant: an input that
// is neither a string nor an array of items, or a messages that is not an
// array of objects.
func ToChat(req *Request) (*ChatRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}
	chat := &ChatRequest{
		Model:    req.Model,
		Stream:   req.Stream,
		Messages: make([]map[string]any, 0, 8),
	}
	chatExtras(req, chat)
	chatTools(req, chat)
	chatToolChoice(req, chat)
	if err := chatInput(req, &chat.Messages); err != nil {
		return nil, err
	}
	return chat, nil
}

// --- parameters -----------------------------------------------------------

// chatExtras copies the parameters that have a chat equivalent. Everything
// else in Request is deliberately unread: store, previous_response_id,
// metadata, top_logprobs, logprobs, truncation, text.verbosity and
// max_input_tokens have no chat side and must not reach an upstream.
func chatExtras(req *Request, chat *ChatRequest) {
	if req.Temperature != nil {
		setExtra(chat, "temperature", *req.Temperature)
	}
	if req.TopP != nil {
		setExtra(chat, "top_p", *req.TopP)
	}
	if req.MaxOutputTokens != nil {
		setExtra(chat, "max_tokens", *req.MaxOutputTokens)
	}
	if req.ParallelToolCalls != nil {
		setExtra(chat, "parallel_tool_calls", *req.ParallelToolCalls)
	}
	if req.User != "" {
		setExtra(chat, "user", req.User)
	}
	if effort := chatReasoningEffort(req.Reasoning); effort != "" {
		setExtra(chat, "reasoning_effort", effort)
	}
	if req.Stream {
		// Required, not optional: the gateway harvests token usage from the
		// final streamed chunk, and chat upstreams omit usage unless asked for
		// it. Upstreams that reject the field answer 4xx and the chain fails
		// over, which is the cheaper of the two errors.
		setExtra(chat, "stream_options", map[string]any{"include_usage": true})
	}
}

// chatReasoningEffort maps a Responses reasoning effort onto the chat field.
// "minimal" means "do not reason", which is what omitting the field already
// means, and "xhigh" has no chat equivalent. Third-party upstreams reject
// effort values outside their own set, so unknown values are dropped rather
// than passed through.
func chatReasoningEffort(r *Reasoning) string {
	if r == nil {
		return ""
	}
	switch r.Effort {
	case "low", "medium", "high":
		return r.Effort
	case "xhigh":
		return "high"
	}
	return ""
}

// setExtra records one translated parameter. Extras is created on demand, so a
// request with no parameters marshals to nothing beyond model/messages/stream.
// A nil value means "omit this field".
func setExtra(chat *ChatRequest, key string, val any) {
	if val == nil {
		return
	}
	if chat.Extras == nil {
		chat.Extras = map[string]any{}
	}
	chat.Extras[key] = val
}

// --- input ----------------------------------------------------------------

// chatInput builds the message list: instructions first, then either the
// client's own messages or its input. Messages wins because it is the
// chat-native escape hatch for clients that already speak both protocols, and
// honouring both would duplicate the transcript.
func chatInput(req *Request, out *[]map[string]any) error {
	if req.Instructions != "" {
		*out = append(*out, chatMessage("system", req.Instructions))
	}
	if hasValue(req.Messages) {
		var msgs []map[string]any
		if err := json.Unmarshal(req.Messages, &msgs); err != nil {
			return fmt.Errorf("messages must be an array of message objects: %w", err)
		}
		*out = append(*out, msgs...)
		return nil
	}
	in, err := decodeJSON(req.Input)
	if err != nil {
		return err
	}
	switch v := in.(type) {
	case nil:
		return nil
	case string:
		*out = append(*out, chatMessage("user", v))
		return nil
	case []any:
		// Calls are grouped: chat wants one assistant message per turn with all
		// of its tool_calls, followed by one tool message per call. Codex emits
		// several calls in a turn, and separate assistant messages would leave
		// those tool outputs not adjacent to the call they answer, which strict
		// upstreams reject outright.
		var calls []map[string]any
		flushCalls := func() {
			if len(calls) == 0 {
				return
			}
			*out = append(*out, map[string]any{
				"role": "assistant", "content": "", "tool_calls": calls,
			})
			calls = nil
		}
		for _, raw := range v {
			if c := chatCallOf(raw); c != nil {
				calls = append(calls, c)
				continue
			}
			flushCalls()
			if m := chatItem(raw); m != nil {
				*out = append(*out, m)
			}
		}
		flushCalls()
		return nil
	}
	return fmt.Errorf("input must be a string or an array of items: got %T", in)
}

// chatItem translates one Responses input item, returning nil for item types
// with no chat equivalent. Dropping rather than inventing a message is what
// keeps role ordering intact for upstreams that enforce alternation, and it
// means a transcript with reasoning items still reads as the turns the client
// actually took.
func chatItem(raw any) map[string]any {
	item, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	t := strOf(item, "type")
	if t == "" {
		// The Responses API defaults an item that carries a role but no type to
		// a message item, and real clients omit the field routinely. Treating it
		// as unknown empties the conversation, which upstreams answer with a 400
		// "Cannot apply chat template to an empty conversation" — the failure
		// only hides when instructions adds a system message first.
		return chatMessageItem(item)
	}
	switch t {
	case "message":
		return chatMessageItem(item)
	case "function_call_output", "custom_tool_call_output":
		return chatOutputItem(item)
	}
	return nil
}

// chatMessageItem maps a message item onto one chat message. developer has no
// chat role, so it becomes system; a role with no chat role at all drops the
// item.
func chatMessageItem(item map[string]any) map[string]any {
	role, ok := chatRole(strOf(item, "role"))
	if !ok {
		return nil
	}
	return map[string]any{"role": role, "content": chatContent(item["content"])}
}

func chatRole(role string) (string, bool) {
	if role == "developer" {
		return "system", true
	}
	switch role {
	case "user", "assistant", "system":
		return role, true
	}
	return "", false
}

// chatMessage builds the simple form of a chat message.
func chatMessage(role, content string) map[string]any {
	return map[string]any{"role": role, "content": content}
}

// chatContent maps a message's content onto chat content: a string when the
// client sent a string or only text parts, a parts array when an image
// survives. Text parts are joined, because a chat content array is only
// accepted for multimodal messages and joining adjacent text is the only form
// every upstream takes.
func chatContent(content any) any {
	switch v := content.(type) {
	case nil:
		return ""
	case string:
		return v
	case []any:
		return chatParts(v)
	}
	return ""
}

// chatParts translates content parts in order, joining adjacent text. A
// text-only array collapses to a plain string, because a chat content array
// is only accepted for multimodal messages and joining is the only form every
// upstream takes. Parts with no chat equivalent are dropped; if that empties
// the array the caller still gets content "" rather than no message at all, so
// the turn stays where it was.
func chatParts(parts []any) any {
	var chat []map[string]any
	var text strings.Builder
	flush := func() {
		if text.Len() == 0 {
			return
		}
		chat = append(chat, map[string]any{"type": "text", "text": text.String()})
		text.Reset()
	}
	multimodal := false
	for _, raw := range parts {
		part, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if img := chatImagePart(part); img != nil {
			multimodal = true
			flush()
			chat = append(chat, img)
			continue
		}
		if s := chatTextOf(part); s != "" {
			text.WriteString(s)
		}
	}
	if !multimodal {
		return text.String()
	}
	flush()
	return chat
}

// chatTextOf returns the text of a text part, or "" for any other part type:
// input_file, output_file, refusal and the rest have nothing to say in chat.
func chatTextOf(part map[string]any) string {
	t := strOf(part, "type")
	if t != "input_text" && t != "output_text" && t != "text" {
		return ""
	}
	return strOf(part, "text")
}

// chatImagePart maps an input_image part onto a chat image_url part. The
// Responses form puts image_url inline, as a string or an object, with detail
// as a sibling; the chat form nests both under image_url, so the object is
// rebuilt rather than copied.
func chatImagePart(part map[string]any) map[string]any {
	if strOf(part, "type") != "input_image" {
		return nil
	}
	url := map[string]any{}
	switch src := part["image_url"].(type) {
	case string:
		url["url"] = src
	case map[string]any:
		for k, v := range src {
			url[k] = v
		}
	}
	if s, ok := url["url"].(string); !ok || s == "" {
		return nil
	}
	if d := strOf(part, "detail"); d != "" {
		url["detail"] = d
	}
	return map[string]any{"type": "image_url", "image_url": url}
}

// --- tool calls -----------------------------------------------------------

// chatCallOf translates a call item into the tool call object that chat groups
// under an assistant message, or nil if the item is not a call.
func chatCallOf(raw any) map[string]any {
	item, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	switch strOf(item, "type") {
	case "function_call", "custom_tool_call":
		return chatCallItem(item)
	}
	return nil
}

// chatCallItem builds the tool call object for one function_call or
// custom_tool_call. call_id wins over id because that is the field the
// matching *_call_output item quotes back.
func chatCallItem(item map[string]any) map[string]any {
	name := strOf(item, "name")
	if name == "" {
		return nil
	}
	call := map[string]any{
		"type":     "function",
		"function": map[string]any{"name": name, "arguments": chatArguments(item)},
	}
	if id := strOf(item, "call_id"); id != "" {
		call["id"] = id
	} else if id := strOf(item, "id"); id != "" {
		call["id"] = id
	}
	return call
}

// chatArguments returns the argument string a chat tool call carries.
// function_call already has one as a JSON string and it passes through
// untouched; custom_tool_call has a JSON object, which is marshalled once.
// The result is always a string, because that is the field clients diff.
func chatArguments(item map[string]any) string {
	if v, ok := item["arguments"]; ok && v != nil {
		if s, isStr := v.(string); isStr {
			return s
		}
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
	}
	if v, ok := item["input"]; ok && v != nil {
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
	}
	return ""
}

// chatOutputItem maps a *_call_output onto a tool message. A tool message
// without a tool_call_id is rejected by every chat upstream, so an output item
// with no call_id is dropped rather than emitted half-formed.
func chatOutputItem(item map[string]any) map[string]any {
	callID := strOf(item, "call_id")
	if callID == "" {
		return nil
	}
	return map[string]any{
		"role":         "tool",
		"tool_call_id": callID,
		"content":      chatOutput(item["output"]),
	}
}

// chatOutput flattens a tool output onto the string a chat tool message
// carries. Strings pass through, content-part arrays are joined, and anything
// else is marshalled so no value is lost to a type guess.
func chatOutput(out any) string {
	switch v := out.(type) {
	case nil:
		return ""
	case string:
		return v
	case []any:
		var parts []string
		for _, raw := range v {
			if part, ok := raw.(map[string]any); ok {
				if s := strOf(part, "text"); s != "" {
					parts = append(parts, s)
				}
			}
		}
		if len(parts) == 0 {
			return jsonString(v)
		}
		return strings.Join(parts, "")
	}
	return jsonString(out)
}

// jsonString marshals a decoded value back to text, returning "" so callers can
// always emit a legal string.
func jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// --- tools and tool_choice -----------------------------------------------

// chatTools keeps only the tools an upstream can serve. The test is name-based
// rather than type-based: Codex declares shell and apply_patch as first-class
// tool types that are really just functions, while web_search, file_search,
// image_generation and code_interpreter carry no name at all, so one test
// keeps and drops both without enumerating their types.
func chatTools(req *Request, chat *ChatRequest) {
	var tools []map[string]any
	for _, raw := range req.Tools {
		if t := chatTool(decodeObject(raw)); t != nil {
			tools = append(tools, t)
		}
	}
	if len(tools) == 0 {
		return
	}
	setExtra(chat, "tools", tools)
}

// chatTool rebuilds a chat function tool. Rebuilding rather than copying also
// drops "strict", which third-party OpenAI-compatible gateways reject.
func chatTool(item map[string]any) map[string]any {
	name := strOf(item, "name")
	if name == "" {
		return nil
	}
	fn := map[string]any{"name": name}
	if desc := strOf(item, "description"); desc != "" {
		fn["description"] = desc
	}
	if params := chatParams(item["parameters"]); params != nil {
		fn["parameters"] = params
	}
	return map[string]any{"type": "function", "function": fn}
}

// chatParams passes a tool's JSON schema through only when it carries
// anything. An empty schema is what a client sends for a tool with no
// arguments, and some upstreams reject a parameters key that is an empty
// object.
func chatParams(params any) any {
	switch v := params.(type) {
	case nil, string:
		return nil
	case map[string]any:
		if len(v) == 0 {
			return nil
		}
	case []any:
		if len(v) == 0 {
			return nil
		}
	}
	return params
}

// chatToolChoice maps a Responses tool_choice onto its chat equivalent,
// omitting the field for shapes chat cannot express. Omitting rather than
// defaulting to "auto" matters: a client that pinned a specific tool must not
// silently be handed a different one.
func chatToolChoice(req *Request, chat *ChatRequest) {
	v, err := decodeJSON(req.ToolChoice)
	if err != nil {
		return // an unparseable tool_choice is dropped, not fatal
	}
	setExtra(chat, "tool_choice", chatToolChoiceOf(v))
}

func chatToolChoiceOf(v any) any {
	switch tc := v.(type) {
	case string:
		switch tc {
		case "auto", "none", "required":
			return tc
		}
	case map[string]any:
		if name := strOf(tc, "name"); name != "" {
			return namedToolChoice(name)
		}
		// allowed_tools is a list of tool objects; the first one that names a
		// function is all chat can express.
		if tools, ok := tc["tools"].([]any); ok {
			for _, raw := range tools {
				if item, ok := raw.(map[string]any); ok {
					if name := strOf(item, "name"); name != "" {
						return namedToolChoice(name)
					}
				}
			}
		}
	}
	return nil
}

func namedToolChoice(name string) any {
	return map[string]any{"type": "function", "function": map[string]any{"name": name}}
}

// --- decoding helpers -----------------------------------------------------

// decodeJSON parses a RawMessage into Go values, keeping numbers as
// json.Number so they marshal back to the exact digits the client sent. Tool
// arguments and JSON schemas carry integers beyond float64 precision, and the
// gateway must not silently rewrite them.
func decodeJSON(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// decodeObject is decodeJSON restricted to a JSON object; anything else yields
// nil, which callers treat as "nothing to translate".
func decodeObject(raw json.RawMessage) map[string]any {
	v, err := decodeJSON(raw)
	if err != nil {
		return nil
	}
	obj, _ := v.(map[string]any)
	return obj
}

// hasValue reports whether a RawMessage holds anything: a missing field decodes
// to a nil slice of bytes and an explicit null to the bytes "null", and both
// must mean the same thing.
func hasValue(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

// strOf reads a string field from a decoded object. A missing key, a key of
// the wrong type, and a nil value all read as absent, which is what the
// Responses wire format produces for every optional field.
func strOf(obj map[string]any, key string) string {
	s, _ := obj[key].(string)
	return s
}
