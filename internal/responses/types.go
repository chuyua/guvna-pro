// Package responses bridges the OpenAI Responses API to the Chat Completions
// API that every Guvna upstream speaks.
//
// No upstream (amd, nvidia, kira) implements /v1/responses: amd's edge
// blackholes the path (connects, then 50s with zero bytes) and nvidia answers
// a clean 404. So Guvna translates in both directions at the gateway:
//
//	POST /v1/responses ── ToChat ──▶ upstream /v1/chat/completions ── FromChat ──▶ 200
//
// Two rules drive the design:
//
//  1. Lossy in only safe directions. Responses features with no chat
//     equivalent (store, previous_response_id, verbosity, web_search tools)
//     are dropped rather than guessed at. Dropping is always better than
//     fabricating.
//  2. The chain loop stays in the server. translate.go and render.go are pure
//     functions with no HTTP or config dependency, so the retry / health /
//     key-rotation machinery written for /v1/chat/completions is reused as-is.
package responses

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Request is the subset of a Responses request that affects the upstream call.
// Everything else is decoded and discarded, so an evolving client never breaks
// the gateway.
type Request struct {
	Model  string          `json:"model"`
	Stream bool            `json:"stream"`
	Input  json.RawMessage `json:"input"`
	// Instructions becomes the leading system message.
	Instructions string            `json:"instructions"`
	Messages     json.RawMessage   `json:"messages"`
	User         string            `json:"user"`
	ToolChoice   json.RawMessage   `json:"tool_choice"`
	Tools        []json.RawMessage `json:"tools"`
	// MaxOutputTokens becomes max_tokens on the chat side.
	MaxOutputTokens    *int64          `json:"max_output_tokens"`
	MaxInputTokens     *int64          `json:"max_input_tokens"`
	Temperature        *float64        `json:"temperature"`
	TopP               *float64        `json:"top_p"`
	TopLogprobs        *int            `json:"top_logprobs"`
	Logprobs           *bool           `json:"logprobs"`
	Store              *bool           `json:"store"`
	ParallelToolCalls  *bool           `json:"parallel_tool_calls"`
	Truncation         string          `json:"truncation"`
	PreviousResponseID string          `json:"previous_response_id"`
	Metadata           json.RawMessage `json:"metadata"`
	Reasoning          *Reasoning      `json:"reasoning"`
	Text               json.RawMessage `json:"text"`
}

// Reasoning carries the client's reasoning request. Effort is one of
// minimal | low | medium | high | xhigh.
type Reasoning struct {
	Effort  string `json:"effort"`
	Summary string `json:"summary"`
}

// ChatRequest is the translated Chat Completions body.
type ChatRequest struct {
	Model    string           `json:"model"`
	Messages []map[string]any `json:"messages"`
	Stream   bool             `json:"stream"`
	// Extras holds the fields that have a chat-side equivalent and are set
	// only when the client supplied them: temperature, top_p, max_tokens,
	// top_logprobs, logprobs, parallel_tool_calls, user, tools, tool_choice,
	// reasoning_effort, stream_options.
	Extras map[string]any `json:"-"`
}

// Marshal emits ChatRequest as a chat completions body: Model, Messages and
// Stream first, then Extras merged in.
func (c *ChatRequest) Marshal() ([]byte, error) {
	body := map[string]any{"model": c.Model, "messages": c.Messages, "stream": c.Stream}
	for k, v := range c.Extras {
		body[k] = v
	}
	return json.Marshal(body)
}

// ChatUsage is the chat-side usage block.
type ChatUsage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	CachedTokens     int64 `json:"cached_tokens"`
	ReasoningTokens  int64 `json:"reasoning_tokens"`
}

// ChatMessage is a parsed assistant delta or final message.
type ChatMessage struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	Refusal   string `json:"refusal"`
	Reasoning string `json:"reasoning"`
	// ToolCalls arrives as a []json.RawMessage because argument strings can be
	// partial during a stream and must survive byte-for-byte.
	ToolCalls []json.RawMessage `json:"tool_calls"`
}

// ChatChunk is one SSE data: payload of a chat completions stream.
type ChatChunk struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Index        int         `json:"index"`
		Delta        ChatMessage `json:"delta"`
		FinishReason *string     `json:"finish_reason"`
	} `json:"choices"`
	Usage *ChatUsage `json:"usage"`
}

// ChatCompletion is a full (non-streaming) chat completions response.
type ChatCompletion struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Created int64  `json:"created"`
	Choices []struct {
		Index        int         `json:"index"`
		Message      ChatMessage `json:"message"`
		FinishReason string      `json:"finish_reason"`
	} `json:"choices"`
	Usage *ChatUsage `json:"usage"`
}

// Response is a Responses-API response object. It is emitted whole by
// response.created / response.in_progress / response.completed and by the
// non-streaming 200 body.
type Response struct {
	ID                 string           `json:"id"`
	Object             string           `json:"object"`
	CreatedAt          int64            `json:"created_at"`
	Model              string           `json:"model"`
	Status             string           `json:"status"`
	Error              any              `json:"error"`
	IncompleteDetails  any              `json:"incomplete_details"`
	Instructions       any              `json:"instructions"`
	Input              any              `json:"input"`
	Metadata           any              `json:"metadata"`
	MaxInputTokens     any              `json:"max_input_tokens"`
	MaxOutputTokens    any              `json:"max_output_tokens"`
	ParallelToolCalls  *bool            `json:"parallel_tool_calls"`
	PreviousResponseID any              `json:"previous_response_id"`
	Reasoning          any              `json:"reasoning"`
	Store              bool             `json:"store"`
	Stream             any              `json:"stream"`
	StreamOptions      any              `json:"stream_options"`
	StreamEventID      any              `json:"stream_event_id"`
	StreamEvents       []any            `json:"stream_events"`
	StreamEventsCount  int              `json:"stream_events_count"`
	ToolChoice         any              `json:"tool_choice"`
	Tools              []any            `json:"tools"`
	ToolOutputs        []any            `json:"tool_outputs"`
	TopLogprobs        *int             `json:"top_logprobs"`
	TopP               *float64         `json:"top_p"`
	Truncation         any              `json:"truncation"`
	Temperature        *float64         `json:"temperature"`
	Text               any              `json:"text"`
	Output             []map[string]any `json:"output"`
	Usage              *Usage           `json:"usage"`
}

// Usage is the Responses-API usage block.
type Usage struct {
	InputTokens         int64                `json:"input_tokens"`
	OutputTokens        int64                `json:"output_tokens"`
	TotalTokens         int64                `json:"total_tokens"`
	InputTokensDetails  *InputTokensDetails  `json:"input_tokens_details,omitempty"`
	OutputTokensDetails *OutputTokensDetails `json:"output_tokens_details,omitempty"`
}

type InputTokensDetails struct {
	CachedTokens int64 `json:"cached_tokens"`
}

type OutputTokensDetails struct {
	AcceptedPredictionTokens int64 `json:"accepted_prediction_tokens"`
	ReasoningTokens          int64 `json:"reasoning_tokens"`
}

// Token usage carried across the translate boundary: the response side needs
// the chat-side numbers to build Usage, and both sides need the same model
// label so the response reports the chain step that actually served it.
type Served struct {
	ChatModel  string // e.g. "self-dploy/GLM-5.3-Flash"
	ChainModel string
}

// NewID returns a prefixed response object id. Response ids must look like
// resp_<22 chars> and must be unique across the stream so clients can key on
// them.
func NewID(prefix string) string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		binary.BigEndian.PutUint32(b[:], uint32(time.Now().UnixNano()))
	}
	return prefix + fmt.Sprintf("%08x%016x", binary.BigEndian.Uint32(b[:]), time.Now().UnixNano())
}

// SSE writes one server-sent event: an event: line, a data: line, a blank
// line, then a flush. Codex reads the `type` field from the data payload, but
// the event: line is part of the wire contract, so both are emitted.
func SSE(w http.ResponseWriter, fl http.Flusher, event string, payload any) error {
	if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
		return err
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	fl.Flush()
	return nil
}

// Done writes the terminal data: [DONE] sentinel that marks the end of a
// Responses stream.
func Done(w http.ResponseWriter, fl http.Flusher) error {
	if _, err := io.WriteString(w, "data: [DONE]\n\n"); err != nil {
		return err
	}
	fl.Flush()
	return nil
}

// ParseRequest decodes a Responses request body, tolerating extra fields.
func ParseRequest(body []byte) (*Request, error) {
	var req Request
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	return &req, nil
}
