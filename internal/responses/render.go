// render.go renders Responses-API output from Chat Completions input.
//
// FromChat covers the whole exchange at once; Stream covers it one
// chat.completion.chunk at a time. Both build their response object through
// responseEnvelope, so a client sees the same shape whether the answer arrives
// as a 200 body or as the response.completed event.
//
// Two invariants Codex checks, each enforced in one place:
//
//   - every event payload carries a "type" equal to its event: line name
//     (Stream.event), and
//   - a response always has at least one output item (Codex's transcript
//     reconstruction breaks on an empty output array).
package responses

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// FromChat builds a complete Responses-API response object from a
// non-streaming chat completions body.
func FromChat(cc *ChatCompletion, orig *Request, served Served) *Response {
	status := "completed"
	if finishReasonOf(cc) == "length" {
		status = "incomplete"
	}
	resp := responseEnvelope(orig, served, status)
	resp.Output = messageOutputItems(cc)
	resp.Usage = chatUsageToUsage(chatUsageOf(cc))
	if status == "incomplete" {
		resp.IncompleteDetails = map[string]any{"reason": "max_output_tokens"}
	}
	return resp
}

// finishReasonOf reports the first choice's finish_reason, or "" when the body
// carries no choices at all.
func finishReasonOf(cc *ChatCompletion) string {
	if cc == nil || len(cc.Choices) == 0 {
		return ""
	}
	return cc.Choices[0].FinishReason
}

// responseEnvelope builds the response object every Responses reply shares,
// echoing only the request fields this gateway accepted. Pointers are passed
// through directly so an absent client field renders as null.
func responseEnvelope(orig *Request, served Served, status string) *Response {
	if orig == nil {
		orig = &Request{}
	}
	instructions := any(nil)
	if orig.Instructions != "" {
		instructions = orig.Instructions
	}
	reasoning := any(nil)
	if orig.Reasoning != nil && orig.Reasoning.Effort != "" {
		reasoning = map[string]any{"effort": orig.Reasoning.Effort}
	}
	return &Response{
		ID:        NewID("resp_"),
		Object:    "response",
		CreatedAt: time.Now().Unix(),
		Model:     modelLabel(served),
		Status:    status,
		Store:     false,

		Instructions:      instructions,
		Metadata:          nil,
		MaxOutputTokens:   orig.MaxOutputTokens,
		ParallelToolCalls: orig.ParallelToolCalls,
		Reasoning:         reasoning,
		Stream:            nil,
		Text:              nil,
		ToolChoice:        nil,
		TopLogprobs:       orig.TopLogprobs,
		TopP:              orig.TopP,
		Truncation:        nil,
		Temperature:       orig.Temperature,

		Tools:        []any{},
		ToolOutputs:  []any{},
		StreamEvents: []any{},
		Output:       []map[string]any{},
	}
}

// modelLabel reports the model label a response should carry: the upstream's
// echo when it supplied one, else the chain step that served the request.
func modelLabel(served Served) string {
	if served.ChatModel != "" {
		return served.ChatModel
	}
	return served.ChainModel
}

// chatUsageOf extracts the usage block, tolerating a body with no choices at
// all.
func chatUsageOf(cc *ChatCompletion) *ChatUsage {
	if cc == nil {
		return nil
	}
	return cc.Usage
}

// chatUsageToUsage maps chat usage onto Responses usage. A nil block still
// yields a zero Usage: Codex reads usage.output_tokens even when the upstream
// reported nothing.
func chatUsageToUsage(u *ChatUsage) *Usage {
	if u == nil {
		return &Usage{}
	}
	out := &Usage{
		InputTokens:  u.PromptTokens,
		OutputTokens: u.CompletionTokens,
		TotalTokens:  u.TotalTokens,
	}
	if u.CachedTokens > 0 {
		out.InputTokensDetails = &InputTokensDetails{CachedTokens: u.CachedTokens}
	}
	if u.ReasoningTokens > 0 {
		out.OutputTokensDetails = &OutputTokensDetails{ReasoningTokens: u.ReasoningTokens}
	}
	return out
}

// chatToolCallRaw is one chat tool_call from a completed message. Arguments are
// kept raw so they survive byte-for-byte into the response.
type chatToolCallRaw struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	} `json:"function"`
}

// chatToolCallDelta is one tool_call element of a streaming delta. Chat streams
// split arguments across chunks, sometimes with an empty first payload.
type chatToolCallDelta struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// messageOutputItems renders the output items of one completed message:
// reasoning, then the message, then one function_call per tool call.
func messageOutputItems(cc *ChatCompletion) []map[string]any {
	if cc == nil || len(cc.Choices) == 0 {
		return []map[string]any{emptyMessageItem()}
	}
	msg := cc.Choices[0].Message

	items := make([]map[string]any, 0, 3)
	if msg.Reasoning != "" {
		items = append(items, reasoningItem(NewID("rs_"), "completed", msg.Reasoning))
	}

	hasMessage := false
	switch {
	case msg.Content != "":
		items = append(items, messageItem(NewID("msg_"), "completed", outputTextPart(msg.Content)))
		hasMessage = true
	case msg.Refusal != "":
		items = append(items, messageItem(NewID("msg_"), "completed", refusalPart(msg.Refusal)))
		hasMessage = true
	}

	hasToolCall := false
	for _, raw := range msg.ToolCalls {
		var tc chatToolCallRaw
		if err := json.Unmarshal(raw, &tc); err != nil {
			continue
		}
		hasToolCall = true
		items = append(items, functionCallItem(NewID("fc_"), tc.ID, tc.Function.Name, tc.Function.Arguments))
	}

	// A message with neither text nor tool calls still gets an item: Codex's
	// transcript reconstruction breaks on a response whose only output is
	// reasoning.
	if !hasMessage && !hasToolCall {
		items = append(items, emptyMessageItem())
	}
	return items
}

func emptyMessageItem() map[string]any {
	return messageItem(NewID("msg_"), "completed", outputTextPart(""))
}

func reasoningItem(itemID, status, text string) map[string]any {
	summary := []map[string]any{}
	if text != "" {
		summary = []map[string]any{{"type": "summary_text", "text": text, "status": status}}
	}
	return map[string]any{
		"type":    "reasoning",
		"id":      itemID,
		"status":  status,
		"summary": summary,
	}
}

func messageItem(itemID, status string, parts ...map[string]any) map[string]any {
	content := make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		content = append(content, p)
	}
	return map[string]any{
		"type":    "message",
		"id":      itemID,
		"status":  status,
		"role":    "assistant",
		"content": content,
	}
}

func outputTextPart(text string) map[string]any {
	return map[string]any{
		"type":        "output_text",
		"text":        text,
		"annotations": []any{},
		"logprobs":    nil,
	}
}

func refusalPart(refusal string) map[string]any {
	return map[string]any{"type": "refusal", "refusal": refusal}
}

func functionCallItem(itemID, callID, name string, arguments json.RawMessage) map[string]any {
	return functionCallItemText(itemID, callID, name, argumentsJSONString(arguments))
}

// functionCallItemText is the same item from an already-normalised argument
// string, used for items accumulated across a stream.
func functionCallItemText(itemID, callID, name, arguments string) map[string]any {
	return map[string]any{
		"type":      "function_call",
		"id":        itemID,
		"call_id":   callID,
		"name":      name,
		"arguments": arguments,
		"status":    "completed",
	}
}

// argumentsJSONString normalises a chat function.arguments value to the JSON
// string Responses requires. Upstreams mostly send a JSON string; the ones
// that send an object get serialised so clients always see a string.
func argumentsJSONString(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return "{}"
	}
	if s[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err == nil {
			// Upstreams send "arguments":"" for a tool with no arguments rather
			// than omitting the field. Codex parses arguments as JSON, so an
			// empty string is a hard failure — normalise it to an empty object.
			if str == "" {
				return "{}"
			}
			return str
		}
	}
	return s
}

// argumentsValue applies the same normalisation to an already-accumulated
// argument string from a stream.
func argumentsValue(s string) string {
	if s == "" {
		return "{}"
	}
	return s
}

// Stream translates a chat.completion.chunk sequence into Responses stream
// events. Each output item gets an output_index when it opens, so indices are
// assigned in ascending order and never reused.
type Stream struct {
	w      http.ResponseWriter
	fl     http.Flusher
	orig   *Request
	served Served

	// id and createdAt are fixed at open time: every event of the stream
	// must reference the same response object.
	id        string
	createdAt int64

	// Open items, keyed by the shape that opened them. openCalls is keyed by
	// the chat tool_calls[].index the upstream multiplexes tool calls by.
	reasoning *openItem
	message   *openItem
	openCalls map[int]*openCall

	outIndex int
	usage    *ChatUsage
}

// openItem tracks one open text-like output item: its id, its output_index and
// the accumulated content of its single part.
type openItem struct {
	itemID string
	index  int
	text   string
}

// openCall tracks one open function_call output item.
type openCall struct {
	itemID    string
	callID    string
	name      string
	arguments string
	index     int
}

// NewStream writes the opening response.created and response.in_progress
// events and returns the translator for subsequent chat chunks.
func NewStream(w http.ResponseWriter, fl http.Flusher, orig *Request, served Served) (*Stream, error) {
	s := &Stream{
		w:         w,
		fl:        fl,
		orig:      orig,
		served:    served,
		id:        NewID("resp_"),
		createdAt: time.Now().Unix(),
		openCalls: make(map[int]*openCall),
	}
	// created and in_progress describe the same response object, so the
	// envelope is built once and reused.
	envelope := s.responseObject("in_progress")
	if err := s.event("response.created", map[string]any{"response": envelope}); err != nil {
		return nil, err
	}
	if err := s.event("response.in_progress", map[string]any{"response": envelope}); err != nil {
		return nil, err
	}
	return s, nil
}

// responseObject renders this stream's response envelope, overriding id and
// created_at so they are stable for the lifetime of the stream.
func (s *Stream) responseObject(status string) *Response {
	resp := responseEnvelope(s.orig, s.served, status)
	resp.ID = s.id
	resp.CreatedAt = s.createdAt
	return resp
}

// event writes one Responses stream event, stamping the payload with the
// event name so the type/event-line invariant holds at every call site.
func (s *Stream) event(name string, payload map[string]any) error {
	payload["type"] = name
	return SSE(s.w, s.fl, name, payload)
}

// nextOutputIndex hands out the next output_index.
func (s *Stream) nextOutputIndex() int {
	n := s.outIndex
	s.outIndex++
	return n
}

// Chunk consumes one chat.completion.chunk JSON object (already stripped of its
// SSE "data:" prefix) and emits the corresponding Responses events.
func (s *Stream) Chunk(chunk *ChatChunk) error {
	if chunk == nil || len(chunk.Choices) == 0 {
		return nil
	}
	// Usage arrives in the final chunk; Finish owns it, so just record it.
	if chunk.Usage != nil {
		s.usage = chunk.Usage
	}
	delta := chunk.Choices[0].Delta
	if err := s.reasoningDelta(delta.Reasoning); err != nil {
		return err
	}
	if err := s.textDelta(delta.Content); err != nil {
		return err
	}
	return s.toolCallDeltas(delta.ToolCalls)
}

func (s *Stream) reasoningDelta(text string) error {
	if text == "" {
		return nil
	}
	it := s.reasoning
	if it == nil {
		it = &openItem{itemID: NewID("rs_")}
		it.index = s.nextOutputIndex()
		s.reasoning = it
		if err := s.event("response.output_item.added", map[string]any{
			"response_id":  s.id,
			"output_index": it.index,
			"item":         reasoningItem(it.itemID, "in_progress", ""),
		}); err != nil {
			return err
		}
		if err := s.event("response.content_part.added", map[string]any{
			"response_id":   s.id,
			"item_id":       it.itemID,
			"output_index":  it.index,
			"content_index": 0,
			"part":          map[string]any{"type": "reasoning_summary_text", "text": ""},
		}); err != nil {
			return err
		}
	}
	it.text += text
	return s.event("response.reasoning_summary_text.delta", map[string]any{
		"response_id":   s.id,
		"item_id":       it.itemID,
		"output_index":  it.index,
		"content_index": 0,
		"delta":         text,
	})
}

func (s *Stream) textDelta(text string) error {
	if text == "" {
		return nil
	}
	if err := s.openMessage(); err != nil {
		return err
	}
	it := s.message
	it.text += text
	return s.event("response.output_text.delta", map[string]any{
		"response_id":   s.id,
		"item_id":       it.itemID,
		"output_index":  it.index,
		"content_index": 0,
		"delta":         text,
	})
}

// openMessage opens the message output item on its first delta, and is reused
// by Finish to guarantee a non-empty output array.
func (s *Stream) openMessage() error {
	if s.message != nil {
		return nil
	}
	it := &openItem{itemID: NewID("msg_")}
	it.index = s.nextOutputIndex()
	s.message = it
	if err := s.event("response.output_item.added", map[string]any{
		"response_id":  s.id,
		"output_index": it.index,
		"item":         messageItem(it.itemID, "in_progress"),
	}); err != nil {
		return err
	}
	return s.event("response.content_part.added", map[string]any{
		"response_id":   s.id,
		"item_id":       it.itemID,
		"output_index":  it.index,
		"content_index": 0,
		"part":          outputTextPart(""),
	})
}

func (s *Stream) toolCallDeltas(raws []json.RawMessage) error {
	for _, raw := range raws {
		var d chatToolCallDelta
		if err := json.Unmarshal(raw, &d); err != nil {
			continue
		}
		tc := s.openCalls[d.Index]
		if tc == nil {
			// A call opens on its first id or name; arguments alone also open
			// it rather than dropping the payload.
			if d.ID == "" && d.Function.Name == "" && d.Function.Arguments == "" {
				continue
			}
			tc = &openCall{itemID: NewID("fc_")}
			tc.index = s.nextOutputIndex()
			s.openCalls[d.Index] = tc
			if err := s.event("response.output_item.added", map[string]any{
				"response_id":  s.id,
				"output_index": tc.index,
				"item": map[string]any{
					"type":      "function_call",
					"id":        tc.itemID,
					"call_id":   d.ID,
					"name":      d.Function.Name,
					"arguments": "",
					"status":    "in_progress",
				},
			}); err != nil {
				return err
			}
		}
		if tc.callID == "" {
			tc.callID = d.ID
		}
		if tc.name == "" {
			tc.name = d.Function.Name
		}
		if d.Function.Arguments == "" {
			continue
		}
		tc.arguments += d.Function.Arguments
		return s.event("response.function_call_arguments.delta", map[string]any{
			"response_id":  s.id,
			"item_id":      tc.itemID,
			"output_index": tc.index,
			"delta":        d.Function.Arguments,
		})
	}
	return nil
}

// Finish closes the stream: it finalises the open message / function_call
// items, emits response.completed with the full response object, then the
// data: [DONE] sentinel. It is safe to call once even if no content was
// emitted. reason is the upstream finish_reason; the Responses wire protocol
// has no field for it, so it is accepted and discarded.
func (s *Stream) Finish(reason string, usage *ChatUsage) error {
	if usage != nil {
		s.usage = usage
	}
	if err := s.closeReasoning(); err != nil {
		return err
	}
	// Nothing streamed at all: the output array would be empty.
	if s.message == nil && s.reasoning == nil && len(s.openCalls) == 0 {
		if err := s.openMessage(); err != nil {
			return err
		}
	}
	if err := s.closeMessage(); err != nil {
		return err
	}
	for _, tc := range s.sortedCalls() {
		if err := s.closeCall(tc); err != nil {
			return err
		}
	}
	status := "completed"
	if reason == "length" {
		// The model hit its output budget. The Responses wire carries no
		// finish_reason field, so status plus incomplete_details are the only
		// signal a client gets that the answer was cut off - reporting
		// completed would make a truncation look like a natural stop.
		status = "incomplete"
	}
	resp := s.responseObject(status)
	resp.Output = s.finalOutput()
	resp.Usage = chatUsageToUsage(s.usage)
	if status == "incomplete" {
		resp.IncompleteDetails = map[string]any{"reason": "max_output_tokens"}
	}
	if err := s.event("response.completed", map[string]any{"response": resp}); err != nil {
		return err
	}
	return Done(s.w, s.fl)
}

// closeReasoning finalises the reasoning item.
func (s *Stream) closeReasoning() error {
	it := s.reasoning
	if it == nil {
		return nil
	}
	if err := s.event("response.reasoning_summary_text.done", map[string]any{
		"response_id":   s.id,
		"item_id":       it.itemID,
		"output_index":  it.index,
		"content_index": 0,
		"text":          it.text,
	}); err != nil {
		return err
	}
	if err := s.event("response.content_part.done", map[string]any{
		"response_id":   s.id,
		"item_id":       it.itemID,
		"output_index":  it.index,
		"content_index": 0,
		"part":          map[string]any{"type": "reasoning_summary_text", "text": it.text},
	}); err != nil {
		return err
	}
	return s.event("response.output_item.done", map[string]any{
		"response_id":  s.id,
		"output_index": it.index,
		"item":         reasoningItem(it.itemID, "completed", it.text),
	})
}

// closeMessage finalises the message item.
func (s *Stream) closeMessage() error {
	it := s.message
	if it == nil {
		return nil
	}
	if it.text != "" {
		if err := s.event("response.output_text.done", map[string]any{
			"response_id":   s.id,
			"item_id":       it.itemID,
			"output_index":  it.index,
			"content_index": 0,
			"text":          it.text,
		}); err != nil {
			return err
		}
	}
	if err := s.event("response.content_part.done", map[string]any{
		"response_id":   s.id,
		"item_id":       it.itemID,
		"output_index":  it.index,
		"content_index": 0,
		"part":          outputTextPart(it.text),
	}); err != nil {
		return err
	}
	return s.event("response.output_item.done", map[string]any{
		"response_id":  s.id,
		"output_index": it.index,
		"item":         messageItem(it.itemID, "completed", outputTextPart(it.text)),
	})
}

// closeCall finalises one function_call item.
func (s *Stream) closeCall(tc *openCall) error {
	args := argumentsValue(tc.arguments)
	if err := s.event("response.function_call_arguments.done", map[string]any{
		"response_id":  s.id,
		"item_id":      tc.itemID,
		"output_index": tc.index,
		"arguments":    args,
	}); err != nil {
		return err
	}
	return s.event("response.output_item.done", map[string]any{
		"response_id":  s.id,
		"output_index": tc.index,
		"item":         functionCallItemText(tc.itemID, tc.callID, tc.name, args),
	})
}

// finalOutput assembles the finished output items in ascending output_index
// order, which is the order they were emitted.
func (s *Stream) finalOutput() []map[string]any {
	var indexed []indexedItem
	if s.reasoning != nil {
		indexed = append(indexed, indexedItem{s.reasoning.index, reasoningItem(s.reasoning.itemID, "completed", s.reasoning.text)})
	}
	if s.message != nil {
		indexed = append(indexed, indexedItem{s.message.index, messageItem(s.message.itemID, "completed", outputTextPart(s.message.text))})
	}
	for _, tc := range s.sortedCalls() {
		indexed = append(indexed, indexedItem{tc.index, functionCallItemText(tc.itemID, tc.callID, tc.name, argumentsValue(tc.arguments))})
	}
	sort.SliceStable(indexed, func(i, j int) bool { return indexed[i].index < indexed[j].index })

	items := make([]map[string]any, len(indexed))
	for i, in := range indexed {
		items[i] = in.item
	}
	return items
}

// sortedCalls returns open function calls in ascending chat tool_calls index.
func (s *Stream) sortedCalls() []*openCall {
	idxs := make([]int, 0, len(s.openCalls))
	for i := range s.openCalls {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	out := make([]*openCall, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, s.openCalls[i])
	}
	return out
}

// Errorf emits response.error followed by the terminal sentinel. No per-item
// done events are emitted: response.error is itself terminal, the client is
// already committed to this stream so there is no failover left to hand it,
// and any item left open is abandoned as an incomplete item. Codex reads the
// error object rather than the output array once it has seen it.
func (s *Stream) Errorf(status int, msg string) error {
	if err := s.event("response.error", map[string]any{
		"response_id": s.id,
		"error": map[string]any{
			"message": msg,
			"code":    strconv.Itoa(status),
		},
	}); err != nil {
		return err
	}
	return Done(s.w, s.fl)
}

// indexedItem pairs a finished output item with the output_index it was
// emitted at.
type indexedItem struct {
	index int
	item  map[string]any
}
