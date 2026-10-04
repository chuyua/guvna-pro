package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

type streamLine struct {
	line []byte
	err  error
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
			line, err := br.ReadBytes('\n')
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
type sseData struct{ lines []string }

func (d *sseData) line(line []byte) (string, bool) {
	text := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
	if text == "" {
		return d.flush()
	}
	field, value, _ := strings.Cut(text, ":")
	if field == "data" {
		value = strings.TrimPrefix(value, " ")
		d.lines = append(d.lines, value)
	}
	return "", false
}

func (d *sseData) flush() (string, bool) {
	if len(d.lines) == 0 {
		return "", false
	}
	data := strings.Join(d.lines, "\n")
	d.lines = nil
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

// streamChunk mirrors the fields aggregation needs from an SSE
// chat.completion.chunk. Delta text fields are pointers so an absent field
// (no delta this chunk) is distinguishable from an empty string.
type streamChunk struct {
	ID      string `json:"id"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Delta struct {
			Content         *string `json:"content"`
			ReasoningText   *string `json:"reasoning_content"`
			Reasoning       *string `json:"reasoning"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage json.RawMessage `json:"usage"`
}

// aggregateStreamed consumes a stream_only provider's SSE response and
// produces the equivalent non-streaming chat.completion response. A stream
// that ends without a terminal finish_reason is an error: same rule as a
// truncated non-streaming body, so the walk marks the step down and moves on.
func (s *Server) aggregateStreamed(resp *http.Response) (*http.Response, error) {
	var id, model, finish string
	var created int64
	var content, reasoning strings.Builder
	var usage json.RawMessage
	sc := bufio.NewScanner(io.LimitReader(resp.Body, 256<<20))
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	d := &sseData{}
	for sc.Scan() {
		data, ok := d.line(sc.Bytes())
		if !ok {
			continue
		}
		if data == "[DONE]" {
			break
		}
		var cm streamChunk
		if err := json.Unmarshal([]byte(data), &cm); err != nil {
			continue
		}
		if cm.ID != "" {
			id = cm.ID
		}
		if cm.Model != "" {
			model = cm.Model
		}
		if cm.Created != 0 {
			created = cm.Created
		}
		if len(cm.Choices) > 0 {
			c := cm.Choices[0]
			if c.Delta.Content != nil {
				content.WriteString(*c.Delta.Content)
			}
			if c.Delta.Reasoning != nil {
				reasoning.WriteString(*c.Delta.Reasoning)
			} else if c.Delta.ReasoningText != nil {
				reasoning.WriteString(*c.Delta.ReasoningText)
			}
			if c.FinishReason != nil && *c.FinishReason != "" {
				finish = *c.FinishReason
			}
		}
		if len(cm.Usage) > 0 && string(cm.Usage) != "null" && string(cm.Usage) != "{}" {
			usage = cm.Usage
		}
	}
	resp.Body.Close()
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if finish == "" {
		return nil, errors.New("stream ended without finish_reason")
	}
	msg := map[string]any{"role": "assistant", "content": content.String()}
	if reasoning.Len() > 0 {
		msg["reasoning_content"] = reasoning.String()
	}
	out := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": created,
		"model":   model,
		"choices": []any{map[string]any{"index": 0, "message": msg, "finish_reason": finish}},
	}
	if len(usage) > 0 {
		var u any
		if err := json.Unmarshal(usage, &u); err == nil && u != nil {
			out["usage"] = u
		}
	}
	body, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
	}, nil
}
