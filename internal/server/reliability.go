// Reliability helpers shared by the chat, embeddings, auto and responses
// walks: failure attribution, cancellable backoff, bounded body reads and
// upstream payload validation.
//
// The gateway has to keep three questions apart when an attempt ends badly:
// did the caller go away, did our own budget run out, or did the upstream
// actually misbehave? Caller cancellation is neutral; gateway timeouts and
// upstream failures count as transient step failures.
package server

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// relayOutcome reports how far a relay got before it returned.
type relayOutcome int

const (
	// relayCommitted means the client response is already (at least partly)
	// written, so the walk must stop: nothing can be safely replaced.
	relayCommitted relayOutcome = iota
	// relayUncommitted means nothing was written to the client, so the walk is
	// still free to try the next step.
	relayUncommitted
)

// relayResult carries the outcome of one relay attempt so the caller can pick
// between stopping, failing over, and propagating the last upstream error.
type relayResult struct {
	outcome    relayOutcome
	clientGone bool
	status     int
	body       []byte
	ct         string
	err        error
}

// walkDeadline returns the wall-clock deadline for a candidate walk, or the
// zero time when the budget is disabled.
func walkDeadline(budget time.Duration) time.Time {
	if budget <= 0 {
		return time.Time{}
	}
	return time.Now().Add(budget)
}

// walkExpired reports whether a walk deadline has passed. The zero deadline
// never expires.
func walkExpired(deadline time.Time) bool {
	return !deadline.IsZero() && time.Now().After(deadline)
}

// stepContext derives the context for one step attempt.
//
// A streaming request keeps the caller's context: a long generation is not a
// failure, and the relay has its own idle watchdog. A non-streaming request
// gets the tighter of the per-step budget and whatever is left of the whole
// walk budget, so a hung upstream is cut off and the walk can move on.
func stepContext(r *http.Request, deadline time.Time, stream bool, stepBudget time.Duration) (context.Context, context.CancelFunc) {
	if stream {
		return r.Context(), func() {}
	}
	ctx := r.Context()
	var cancels []context.CancelFunc
	if stepBudget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, stepBudget)
		cancels = append(cancels, cancel)
	}
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		cancels = append(cancels, cancel)
	}
	return ctx, func() {
		for _, cancel := range cancels {
			cancel()
		}
	}
}

// clientGoneError marks a failure caused by the caller (disconnect, its own
// deadline, or a canceled stream). The walk stops at once and neither the
// provider's health nor its keys are charged for it.
type clientGoneError struct{ err error }

func (e *clientGoneError) Error() string { return "client gone: " + e.err.Error() }
func (e *clientGoneError) Unwrap() error { return e.err }

// stepTimeoutError marks a failure caused by the gateway's own per-step or
// whole-request budget. The upstream may be perfectly healthy; this attempt
// simply could not finish inside the budget, so the walk moves on and the
// step is counted as a transient failure.
type stepTimeoutError struct{ err error }

func (e *stepTimeoutError) Error() string { return "step budget exhausted: " + e.err.Error() }
func (e *stepTimeoutError) Unwrap() error { return e.err }

func isClientGone(err error) bool {
	var target *clientGoneError
	return errors.As(err, &target)
}

// abortCause classifies a context that has ended. When the caller's own
// request context is done the attempt is attributed to the client and must not
// touch provider health or key state; otherwise the gateway's own budget ran
// out and the attempt counts as a transient upstream timeout. Returns nil when
// ctx is still live.
func abortCause(ctx context.Context, r *http.Request) error {
	if ctx.Err() == nil {
		return nil
	}
	if r != nil {
		if cerr := r.Context().Err(); cerr != nil {
			return &clientGoneError{err: cerr}
		}
	}
	return &stepTimeoutError{err: ctx.Err()}
}

// budgets. Zero disables the corresponding limit. Tests override these
// package variables directly; operators override them through the environment.
var (
	// chatStepBudget caps one non-streaming step, including the body read.
	chatStepBudget = envDurationOpt("GUVNA_STEP_BUDGET", 180*time.Second)
	// chatTotalBudget caps the whole non-streaming candidate walk.
	chatTotalBudget = envDurationOpt("GUVNA_REQUEST_BUDGET", 300*time.Second)
	// streamIdle aborts a stream that has produced no complete upstream line for
	// this long. Downstream keep-alive frames do not count as upstream progress.
	streamIdle = envDurationOpt("GUVNA_STREAM_IDLE_TIMEOUT", 120*time.Second)
	// streamMaxDuration caps an active relay independently of upstream progress.
	// It starts after successful headers, so it does not shorten candidate walks.
	streamMaxDuration = envDurationOpt("GUVNA_STREAM_MAX_DURATION", 10*time.Minute)
	// maxResponseBytes bounds a non-streaming upstream body.
	maxResponseBytes = envInt64("GUVNA_MAX_BODY_BYTES", 32<<20)
	// requireStreamTerminator decides whether a chat SSE that ends without
	// a valid finish_reason counts as a failure, even with [DONE]. It defaults to true; the
	// knob exists for providers with a non-standard stream ending.
	requireStreamTerminator = envBool("GUVNA_REQUIRE_STREAM_TERMINATOR", true)
)

func streamDurationTimer() (time.Time, <-chan time.Time, func()) {
	if streamMaxDuration <= 0 {
		return time.Time{}, nil, func() {}
	}
	deadline := time.Now().Add(streamMaxDuration)
	timer := time.NewTimer(time.Until(deadline))
	return deadline, timer.C, func() { timer.Stop() }
}

func streamDurationError() error {
	return &stepTimeoutError{err: fmt.Errorf("stream exceeded maximum duration %s: %w", streamMaxDuration, context.DeadlineExceeded)}
}

func envInt64(name string, def int64) int64 {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// envDurationOpt is envDuration but accepts zero, which disables a limit.
func envDurationOpt(name string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return def
	}
	return d
}

func envBool(name string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

// sleepCtx waits for d but returns as soon as ctx ends, so a request that was
// canceled during a backoff does not sit out the whole delay.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ctx.Err()
	}
}

// readLimited reads at most max+1 bytes so an oversized body is reported as an
// error instead of being buffered without bound or silently truncated.
func readLimited(r io.Reader, max int64) ([]byte, error) {
	if max <= 0 {
		return io.ReadAll(r)
	}
	body, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return body, err
	}
	if int64(len(body)) > max {
		return body, fmt.Errorf("upstream body exceeds %d bytes", max)
	}
	return body, nil
}

// Error bodies are buffered even for streaming requests. Those requests keep
// their caller context, so close a stalled error body when its read budget ends.
func readErrorBody(resp *http.Response, stream bool) ([]byte, error) {
	budget := chatStepBudget
	if stream {
		budget = streamIdle
	}
	if budget > 0 {
		timer := time.AfterFunc(budget, func() { resp.Body.Close() })
		defer timer.Stop()
	}
	return readLimited(resp.Body, maxResponseBytes)
}

func isJSONNull(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s == "" || s == "null"
}

// validateChatCompletion rejects a 2xx body that cannot be a chat completion:
// empty, non-JSON, an array, an {"error":...} object sent with status 200, or
// no choices at all. A body that fails this check is a failed attempt, so the
// walk can use the next step instead of handing the caller a broken success.
func validateChatCompletion(body []byte, expectedChoices ...int) error {
	expected := 0
	if len(expectedChoices) > 0 {
		expected = expectedChoices[0]
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return fmt.Errorf("upstream 2xx body is not a JSON object: %w", err)
	}
	if raw, ok := m["error"]; ok && !isJSONNull(raw) {
		return fmt.Errorf("upstream 2xx body carries an error object: %s", truncate(raw, 200))
	}
	choices, ok := m["choices"]
	if !ok || isJSONNull(choices) {
		return errors.New("upstream 2xx body has no choices")
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(choices, &arr); err != nil {
		return errors.New("upstream 2xx body has a non-array choices field")
	}
	if len(arr) == 0 {
		return errors.New("upstream 2xx body has an empty choices array")
	}
	if expected > 1 && len(arr) != expected {
		return errors.New("upstream completion has an unexpected choice count")
	}
	seen := make(map[int]bool)
	for _, raw := range arr {
		var choice struct {
			Index   json.RawMessage            `json:"index"`
			Message map[string]json.RawMessage `json:"message"`
		}
		if err := json.Unmarshal(raw, &choice); err != nil || len(choice.Message) == 0 {
			return errors.New("upstream choice has no message")
		}
		if expected > 1 {
			var index int
			if isJSONNull(choice.Index) || json.Unmarshal(choice.Index, &index) != nil || index < 0 || index >= expected || seen[index] {
				return errors.New("upstream completion has invalid or duplicate choice index")
			}
			seen[index] = true
		}
		usable := false
		for _, field := range []string{"content", "reasoning_content", "reasoning", "refusal"} {
			if value, ok := choice.Message[field]; ok && !isJSONNull(value) {
				var text string
				if err := json.Unmarshal(value, &text); err != nil {
					return fmt.Errorf("upstream message %s is not text", field)
				}
				usable = true
			}
		}
		if value, ok := choice.Message["tool_calls"]; ok && !isJSONNull(value) {
			var calls []json.RawMessage
			if err := json.Unmarshal(value, &calls); err != nil || len(calls) == 0 {
				return errors.New("upstream message has invalid tool calls")
			}
			for _, call := range calls {
				var tool struct {
					ID       string `json:"id"`
					Function struct {
						Name      string  `json:"name"`
						Arguments *string `json:"arguments"`
					} `json:"function"`
				}
				if err := json.Unmarshal(call, &tool); err != nil || tool.ID == "" || tool.Function.Name == "" || tool.Function.Arguments == nil {
					return errors.New("upstream message has malformed tool call")
				}
			}
			usable = true
		}
		if !usable {
			return errors.New("upstream message has no usable result")
		}
	}
	return nil
}

// validateEmbeddings rejects a 2xx body with no usable embedding data.
// Every entry must contain a nonempty numeric vector or base64 float32 bytes.
func validateEmbeddings(body []byte) error {
	var m struct {
		Data  []json.RawMessage `json:"data"`
		Error json.RawMessage   `json:"error"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return fmt.Errorf("upstream 2xx body is not JSON: %w", err)
	}
	if !isJSONNull(m.Error) {
		return errors.New("upstream 2xx body carries an error object")
	}
	if len(m.Data) == 0 {
		return errors.New("upstream 2xx body has no embedding data")
	}
	for _, raw := range m.Data {
		var entry struct {
			Embedding json.RawMessage `json:"embedding"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil || isJSONNull(entry.Embedding) {
			return errors.New("upstream embedding entry has no vector")
		}
		var vector []float64
		if strings.HasPrefix(strings.TrimSpace(string(entry.Embedding)), "[") {
			var numbers []json.RawMessage
			if err := json.Unmarshal(entry.Embedding, &numbers); err != nil || len(numbers) == 0 {
				return errors.New("upstream embedding vector is empty or malformed")
			}
			for _, number := range numbers {
				var value float64
				if isJSONNull(number) || json.Unmarshal(number, &value) != nil {
					return errors.New("upstream embedding vector contains a non-number")
				}
				vector = append(vector, value)
			}
		} else {
			var encoded string
			if json.Unmarshal(entry.Embedding, &encoded) != nil {
				return errors.New("upstream embedding vector is neither numeric array nor base64")
			}
			decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
			if err != nil || len(decoded) == 0 || len(decoded)%4 != 0 {
				return errors.New("upstream embedding vector is not base64 float32")
			}
			for i := 0; i < len(decoded); i += 4 {
				vector = append(vector, float64(math.Float32frombits(binary.LittleEndian.Uint32(decoded[i:i+4]))))
			}
		}
		for _, value := range vector {
			if math.IsInf(value, 0) || math.IsNaN(value) {
				return errors.New("upstream embedding vector contains a non-finite number")
			}
		}
	}
	return nil
}

func unusableBody(err error) relayResult {
	return relayResult{outcome: relayUncommitted, status: http.StatusBadGateway,
		ct: "application/json", body: []byte(`{"error":{"message":"upstream returned an unusable response"}}`), err: err}
}

// chatSSEKind classifies one SSE data payload from a chat completions stream.
type chatSSEKind int

const (
	chatSSEChunk chatSSEKind = iota
	chatSSEDone
)

// classifyChatSSE validates one upstream SSE payload before it is relayed.
// The payload must be [DONE] or a JSON object; an explicit error object is a
// failure, and so is a payload that is not JSON at all (a truncated frame).
// The boolean reports whether the payload carried a terminal finish_reason.
func classifyChatSSE(payload string) (chatSSEKind, bool, error) {
	if strings.TrimSpace(payload) == "[DONE]" {
		return chatSSEDone, false, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		return chatSSEChunk, false, fmt.Errorf("upstream SSE data is not JSON: %w", err)
	}
	if m == nil {
		return chatSSEChunk, false, errors.New("upstream SSE data is null")
	}
	if raw, ok := m["error"]; ok && !isJSONNull(raw) {
		return chatSSEChunk, false, fmt.Errorf("upstream SSE error payload: %s", truncate(raw, 200))
	}
	choices, ok := m["choices"]
	if !ok || isJSONNull(choices) {
		if usage, ok := m["usage"]; ok && !isJSONNull(usage) {
			var stats map[string]json.RawMessage
			if json.Unmarshal(usage, &stats) == nil && len(stats) > 0 {
				return chatSSEChunk, false, nil
			}
		}
		return chatSSEChunk, false, errors.New("upstream SSE data has no choices or usage")
	}
	var arr []struct {
		FinishReason *string                    `json:"finish_reason"`
		Delta        map[string]json.RawMessage `json:"delta"`
	}
	if err := json.Unmarshal(choices, &arr); err != nil {
		return chatSSEChunk, false, errors.New("upstream SSE choices are malformed")
	}
	if len(arr) == 0 {
		if usage, ok := m["usage"]; ok && !isJSONNull(usage) {
			return chatSSEChunk, false, nil
		}
		return chatSSEChunk, false, errors.New("upstream SSE has empty choices")
	}
	finished := false
	for _, c := range arr {
		if c.Delta == nil {
			return chatSSEChunk, false, errors.New("upstream SSE choice has no delta")
		}
		if c.FinishReason != nil && *c.FinishReason != "" {
			switch *c.FinishReason {
			case "stop", "length", "content_filter", "tool_calls", "function_call":
				finished = true
			default:
				return chatSSEChunk, false, errors.New("upstream SSE has unknown finish_reason")
			}
		}
	}
	return chatSSEChunk, finished, nil
}

// chatSSEProgress keeps terminal state per choice. A finish_reason for one
// choice cannot complete another choice or stand in for a missing requested
// choice. n <= 1 retains compatibility with providers returning extra choices.
type chatSSEProgress struct {
	expected int
	choices  map[int]string
}

func (p *chatSSEProgress) observe(payload string) (chatSSEKind, error) {
	kind, _, err := classifyChatSSE(payload)
	if err != nil || kind == chatSSEDone {
		return kind, err
	}
	var chunk struct {
		Choices []struct {
			Index        *int                       `json:"index"`
			Delta        map[string]json.RawMessage `json:"delta"`
			FinishReason *string                    `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
		return kind, err
	}
	if p.choices == nil {
		p.choices = make(map[int]string)
	}
	seen := make(map[int]bool, len(chunk.Choices))
	for _, c := range chunk.Choices {
		index := 0
		if c.Index != nil {
			index = *c.Index
		} else if len(chunk.Choices) != 1 || p.expected > 1 {
			return kind, errors.New("upstream multi-choice chunk has no valid index")
		}
		if index < 0 || seen[index] {
			return kind, errors.New("upstream chunk has invalid or duplicate choice index")
		}
		if p.expected > 1 && index >= p.expected {
			return kind, errors.New("upstream choice index exceeds requested n")
		}
		seen[index] = true
		finish := p.choices[index]
		if finish != "" {
			for _, raw := range c.Delta {
				var value any
				if err := json.Unmarshal(raw, &value); err != nil {
					return kind, err
				}
				switch v := value.(type) {
				case nil:
					continue
				case string:
					if v == "" {
						continue
					}
				case []any:
					if len(v) == 0 {
						continue
					}
				case map[string]any:
					if len(v) == 0 {
						continue
					}
				}
				return kind, errors.New("upstream choice carried data after finish_reason")
			}
		}
		if c.FinishReason != nil && *c.FinishReason != "" {
			if finish != "" && finish != *c.FinishReason {
				return kind, errors.New("upstream choice has conflicting finish_reason")
			}
			finish = *c.FinishReason
		}
		p.choices[index] = finish
	}
	return kind, nil
}

func (p *chatSSEProgress) complete(requireFinish bool) error {
	if p.expected > 1 && len(p.choices) != p.expected {
		return errors.New("upstream stream omitted a requested choice")
	}
	// The non-standard single-choice opt-out remains available, but it cannot
	// hide a missing or unfinished choice in a multi-choice response.
	if !requireFinish && p.expected <= 1 && len(p.choices) <= 1 {
		return nil
	}
	if len(p.choices) == 0 {
		return errors.New("upstream stream ended without a finish_reason")
	}
	for _, finish := range p.choices {
		if finish == "" {
			return errors.New("upstream stream choice ended without a finish_reason")
		}
	}
	return nil
}
