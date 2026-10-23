package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

type streamLine struct {
	line []byte
	err  error
}

// Bound both an individual line and a multi-line SSE event. A generation may
// contain arbitrarily many bounded events, without buffering the whole stream.
const maxStreamEventBytes = 8 << 20

func readBoundedStreamLine(br *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, err := br.ReadSlice('\n')
		if len(line)+len(part) > maxStreamEventBytes {
			return nil, errors.New("upstream SSE line exceeds size limit")
		}
		line = append(line, part...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}

type streamOutput struct {
	http.ResponseWriter
	wrote bool
}

func (w *streamOutput) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if n > 0 {
		w.wrote = true
	}
	return n, err
}

func resetKeepAlive(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(KeepAliveInterval)
}

// readStreamLines separates blocking upstream reads from the relay's timer.
// The relay remains the only response writer. stop closes the HTTP response
// body (unblocking Read) and waits for the reader, including a blocked send.
func readStreamLines(ctx context.Context, body io.ReadCloser) (<-chan streamLine, func()) {
	lines := make(chan streamLine)
	stopped := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(lines)
		br := bufio.NewReader(body)
		for {
			line, err := readBoundedStreamLine(br)
			select {
			case lines <- streamLine{line, err}:
			case <-ctx.Done():
				return
			case <-stopped:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return lines, func() {
		close(stopped)
		body.Close()
		<-done
	}
}

// sseData dispatches data only at the event's blank line. SSE joins multiple
// data fields with a newline; comment, event, id and retry fields carry no data.
type sseData struct {
	data    strings.Builder
	hasData bool
}

func (d *sseData) line(line []byte) (string, bool, error) {
	text := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
	if len(text) > maxStreamEventBytes {
		return "", false, errors.New("upstream SSE line exceeds size limit")
	}
	if text == "" {
		payload, ok := d.flush()
		return payload, ok, nil
	}
	field, value, _ := strings.Cut(text, ":")
	if field == "data" {
		value = strings.TrimPrefix(value, " ")
		size := d.data.Len() + len(value)
		if d.hasData {
			size++ // SSE joins data fields with one newline.
		}
		if size > maxStreamEventBytes {
			d.data.Reset()
			d.hasData = false
			return "", false, errors.New("upstream SSE event exceeds size limit")
		}
		if d.hasData {
			d.data.WriteByte('\n')
		}
		d.data.WriteString(value)
		d.hasData = true
	}
	return "", false, nil
}

func (d *sseData) flush() (string, bool) {
	if !d.hasData {
		return "", false
	}
	data := d.data.String()
	d.data.Reset()
	d.hasData = false
	return data, true
}

// forceStreamBody rewrites a non-streaming chat request to upstream
// streaming, so a stream_only provider can serve it. include_usage asks the
// upstream for a usage chunk; upstreams that ignore it simply omit usage in
// the aggregated result.
func forceStreamBody(body []byte) []byte {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	m["stream"] = true
	m["stream_options"] = map[string]any{"include_usage": true}
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// streamToolCall is one tool-call fragment. Providers split a single call
// across many chunks: index is stable per call, while identity fields and
// JSON arguments may arrive in pieces that must be concatenated in order.
type streamToolCall struct {
	Index    *int   `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// streamChunk mirrors the fields aggregation needs from an SSE
// chat.completion.chunk. Delta text fields are pointers so an absent field
// (no delta this chunk) is distinguishable from an empty string.
type streamChunk struct {
	ID      string `json:"id"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Index *int `json:"index"`
		Delta struct {
			Content       *string          `json:"content"`
			ReasoningText *string          `json:"reasoning_content"`
			Reasoning     *string          `json:"reasoning"`
			ToolCalls     []streamToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage json.RawMessage `json:"usage"`
	Error json.RawMessage `json:"error"`
}

// toolCallAgg accumulates one tool call across chunks.
type toolCallAgg struct {
	id, typ, name string
	args          strings.Builder
}

func (a *toolCallAgg) append(tc streamToolCall) {
	a.id += tc.ID
	a.typ += tc.Type
	a.name += tc.Function.Name
	a.args.WriteString(tc.Function.Arguments)
}

func (a *toolCallAgg) valid() bool {
	return a.id != "" && a.typ == "function" && a.name != "" && json.Valid([]byte(a.args.String()))
}

// choiceAgg keeps output and tool indices local to one completion choice.
type choiceAgg struct {
	content, reasoning strings.Builder
	finish             string
	calls              map[int]*toolCallAgg
	callOrder          []int
}

// aggregateStreamed consumes a stream_only provider's SSE response and
// produces the equivalent non-streaming chat.completion response. A stream
// that ends without a terminal finish_reason is an error: same rule as a
// truncated non-streaming body, so the walk marks the step down and moves on.
func (s *Server) aggregateStreamed(resp *http.Response, expectedChoices ...int) (*http.Response, error) {
	defer resp.Body.Close()
	expected := 0
	if len(expectedChoices) > 0 {
		expected = expectedChoices[0]
	}
	var id, model string
	var created int64
	var usage json.RawMessage
	var usageSize int64
	choices := make(map[int]*choiceAgg)
	var choiceOrder []int
	limit := maxResponseBytes
	budget := aggregateBudget{limit: limit}
	if err := budget.add(int64(len(`{"id":"","object":"chat.completion","created":0,"model":"","choices":[]}`))); err != nil {
		return nil, err
	}
	var input io.Reader = resp.Body
	eventLimit := maxStreamEventBytes
	if limit > 0 {
		input = &aggregateReader{r: resp.Body, remaining: limit, limit: limit}
		if limit < int64(eventLimit) {
			eventLimit = int(limit)
		}
	}
	sc := bufio.NewScanner(input)
	sc.Buffer(make([]byte, 0, min(64*1024, eventLimit+1)), eventLimit+1)
	d := &sseData{}
	consume := func(data string) (bool, error) {
		if strings.TrimSpace(data) == "[DONE]" {
			return true, nil
		}
		if err := checkAggregateStructures(data, limit); err != nil {
			return false, err
		}
		var cm streamChunk
		if err := json.Unmarshal([]byte(data), &cm); err != nil {
			return false, errors.New("invalid upstream SSE JSON")
		}
		if !isJSONNull(cm.Error) {
			return false, errors.New("upstream SSE error payload")
		}
		if len(cm.Choices) == 0 && isJSONNull(cm.Usage) {
			return false, errors.New("upstream SSE payload has no choices or usage")
		}
		if cm.ID != "" {
			if err := budget.add(quotedJSONSize(cm.ID) - quotedJSONSize(id)); err != nil {
				return false, err
			}
			id = cm.ID
		}
		if cm.Model != "" {
			if err := budget.add(quotedJSONSize(cm.Model) - quotedJSONSize(model)); err != nil {
				return false, err
			}
			model = cm.Model
		}
		if cm.Created != 0 {
			if err := budget.add(int64(len(strconv.FormatInt(cm.Created, 10)) - len(strconv.FormatInt(created, 10)))); err != nil {
				return false, err
			}
			created = cm.Created
		}
		seen := make(map[int]bool, len(cm.Choices))
		for _, c := range cm.Choices {
			idx := 0
			if c.Index != nil {
				idx = *c.Index
			} else if len(cm.Choices) != 1 || expected > 1 {
				return false, errors.New("upstream multi-choice chunk has no valid index")
			}
			if idx < 0 || seen[idx] {
				return false, errors.New("upstream chunk has invalid or duplicate choice index")
			}
			if expected > 1 && idx >= expected {
				return false, errors.New("upstream choice index exceeds requested n")
			}
			seen[idx] = true
			choice := choices[idx]
			if choice == nil {
				size := len(aggregateChoiceJSON) + len(strconv.Itoa(idx)) - 1
				if len(choiceOrder) > 0 {
					size++
				}
				if err := budget.add(int64(size)); err != nil {
					return false, err
				}
				choice = &choiceAgg{calls: make(map[int]*toolCallAgg)}
				choices[idx] = choice
				choiceOrder = append(choiceOrder, idx)
			}
			if choice.finish != "" && (c.Delta.Content != nil && *c.Delta.Content != "" || c.Delta.Reasoning != nil && *c.Delta.Reasoning != "" || c.Delta.ReasoningText != nil && *c.Delta.ReasoningText != "" || len(c.Delta.ToolCalls) > 0) {
				return false, errors.New("upstream choice carried data after finish_reason")
			}
			if c.Delta.Content != nil {
				if err := budget.add(quotedJSONSize(*c.Delta.Content)); err != nil {
					return false, err
				}
				choice.content.WriteString(*c.Delta.Content)
			}
			var reasoning string
			if c.Delta.Reasoning != nil {
				reasoning = *c.Delta.Reasoning
			} else if c.Delta.ReasoningText != nil {
				reasoning = *c.Delta.ReasoningText
			}
			if reasoning != "" {
				size := quotedJSONSize(reasoning)
				if choice.reasoning.Len() == 0 {
					size += int64(len(`,"reasoning_content":""`))
				}
				if err := budget.add(size); err != nil {
					return false, err
				}
				choice.reasoning.WriteString(reasoning)
			}
			for _, tc := range c.Delta.ToolCalls {
				if tc.Index == nil || *tc.Index < 0 {
					return false, errors.New("upstream tool call has no valid index")
				}
				idx := *tc.Index
				agg := choice.calls[idx]
				if agg == nil {
					size := len(aggregateToolJSON)
					if len(choice.callOrder) == 0 {
						size += len(`,"tool_calls":[]`)
					} else {
						size++
					}
					if err := budget.add(int64(size)); err != nil {
						return false, err
					}
					agg = &toolCallAgg{}
					choice.calls[idx] = agg
					choice.callOrder = append(choice.callOrder, idx)
				}
				if err := budget.add(quotedJSONSize(tc.ID) + quotedJSONSize(tc.Type) + quotedJSONSize(tc.Function.Name) + quotedJSONSize(tc.Function.Arguments)); err != nil {
					return false, err
				}
				agg.append(tc)
			}
			if c.FinishReason != nil && *c.FinishReason != "" {
				switch *c.FinishReason {
				case "stop", "length", "tool_calls", "function_call", "content_filter":
				default:
					return false, errors.New("unknown upstream finish_reason")
				}
				if choice.finish != "" && choice.finish != *c.FinishReason {
					return false, errors.New("upstream choice has conflicting finish_reason")
				}
				if err := budget.add(quotedJSONSize(*c.FinishReason) - quotedJSONSize(choice.finish)); err != nil {
					return false, err
				}
				choice.finish = *c.FinishReason
			}
		}
		if len(cm.Usage) > 0 && string(cm.Usage) != "null" && string(cm.Usage) != "{}" {
			size := rawJSONSize(cm.Usage)
			change := size - usageSize
			if len(usage) == 0 {
				change += int64(len(`,"usage":`))
			}
			if err := budget.add(change); err != nil {
				return false, err
			}
			usageSize = size
			usage = cm.Usage
		}
		return false, nil
	}
	done := false
	for sc.Scan() {
		data, ok, err := d.line(sc.Bytes())
		if err != nil {
			return nil, err
		}
		if ok {
			var err error
			done, err = consume(data)
			if err != nil {
				return nil, err
			}
			if done {
				break
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if !done {
		if data, ok := d.flush(); ok {
			if _, err := consume(data); err != nil {
				return nil, err
			}
		}
	}
	if len(choiceOrder) == 0 {
		return nil, errors.New("stream ended without finish_reason")
	}
	if expected > 1 && len(choiceOrder) != expected {
		return nil, errors.New("upstream stream omitted a requested choice")
	}
	sort.Ints(choiceOrder)
	outChoices := make([]aggregateChoice, 0, len(choiceOrder))
	for _, index := range choiceOrder {
		choice := choices[index]
		if choice.finish == "" {
			return nil, errors.New("stream ended without finish_reason")
		}
		// A terminal tool_calls reason with no aggregated call is a broken
		// choice, not a text answer: fail the whole uncommitted response.
		if (choice.finish == "tool_calls" || choice.finish == "function_call") && len(choice.callOrder) == 0 {
			return nil, errors.New("stream reported tool_calls but carried no tool call")
		}
		msg := aggregateMessage{Role: "assistant", Content: choice.content.String(), ReasoningContent: choice.reasoning.String()}
		if len(choice.callOrder) > 0 {
			sort.Ints(choice.callOrder)
			out := make([]aggregateTool, 0, len(choice.callOrder))
			for _, idx := range choice.callOrder {
				agg := choice.calls[idx]
				if !agg.valid() {
					return nil, errors.New("upstream tool call is incomplete or invalid")
				}
				out = append(out, aggregateTool{ID: agg.id, Type: agg.typ, Function: aggregateToolFunction{Name: agg.name, Arguments: agg.args.String()}})
			}
			msg.ToolCalls = out
		}
		outChoices = append(outChoices, aggregateChoice{Index: index, Message: msg, FinishReason: choice.finish})
	}
	out := aggregateResponse{ID: id, Object: "chat.completion", Created: created, Model: model, Choices: outChoices, Usage: usage}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	if limit > 0 && int64(len(body)) > limit {
		return nil, errors.New("aggregated upstream response exceeds body limit")
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}, nil
}
