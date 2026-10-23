package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

// aggregateReader probes one byte past the input budget instead of turning a
// full LimitReader into an apparently successful EOF. A completed [DONE] event
// still ends consumption immediately, as it does for ordinary streaming.
type aggregateReader struct {
	r         io.Reader
	remaining int64
	limit     int64
}

func (r *aggregateReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		var extra [1]byte
		n, err := r.r.Read(extra[:])
		if n > 0 {
			return 0, fmt.Errorf("upstream SSE input exceeds %d bytes", r.limit)
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	r.remaining -= int64(n)
	return n, err
}

// aggregateBudget accounts for the final JSON before retaining each fragment.
// Structural costs also bound the number of choices and tools, even when their
// text is empty. The output uses typed records to avoid a map per JSON object.
type aggregateBudget struct {
	limit int64
	size  int64
}

func (b *aggregateBudget) add(n int64) error {
	if b.limit > 0 && n > b.limit-b.size {
		return fmt.Errorf("aggregated upstream response exceeds %d bytes", b.limit)
	}
	b.size += n
	return nil
}

const aggregateChoiceJSON = `{"index":0,"message":{"role":"assistant","content":""},"finish_reason":""}`
const aggregateToolJSON = `{"id":"","type":"","function":{"name":"","arguments":""}}`

// checkAggregateStructures bounds the slices json.Unmarshal is about to build.
// Otherwise an event containing millions of empty choice/tool objects can use
// much more memory than its wire size before the normal validation rejects it.
// Tokens are walked without retaining an object graph. Repeated tool fragments
// in one event share this temporary structure budget even if they use one index.
func checkAggregateStructures(data string, limit int64) error {
	if limit <= 0 {
		return nil
	}
	// Use the decoder's normal syntax/depth limit before the recursive token
	// walk; Token alone need not enforce Unmarshal's nesting-depth limit.
	if !json.Valid([]byte(data)) {
		return errors.New("invalid upstream SSE JSON")
	}
	const (
		ignored = iota
		root
		choices
		choice
		delta
		tools
	)
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	budget := aggregateBudget{limit: limit}
	var walk func(int) error
	walk = func(where int) error {
		token, err := dec.Token()
		if err != nil {
			return err
		}
		switch token {
		case json.Delim('{'):
			for dec.More() {
				key, err := dec.Token()
				if err != nil {
					return err
				}
				name, _ := key.(string)
				next := ignored
				switch {
				case where == root && strings.EqualFold(name, "choices"):
					next = choices
				case where == choice && strings.EqualFold(name, "delta"):
					next = delta
				case where == delta && strings.EqualFold(name, "tool_calls"):
					next = tools
				}
				if err := walk(next); err != nil {
					return err
				}
			}
			_, err = dec.Token()
			return err
		case json.Delim('['):
			for dec.More() {
				next, size := ignored, 0
				if where == choices {
					next, size = choice, len(aggregateChoiceJSON)
				} else if where == tools {
					size = len(aggregateToolJSON)
				}
				if err := budget.add(int64(size)); err != nil {
					return err
				}
				if err := walk(next); err != nil {
					return err
				}
			}
			_, err = dec.Token()
			return err
		}
		return nil
	}
	return walk(root)
}

// quotedJSONSize excludes the two quotes, so independently received string
// fragments can be charged before concatenation. Match encoding/json's default
// HTML escaping, control characters, invalid UTF-8, and JSON line separators.
func quotedJSONSize(s string) int64 {
	var n int64
	for _, r := range s {
		switch r {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			n += 2
		case '<', '>', '&', '\u2028', '\u2029':
			n += 6
		default:
			if r < 0x20 {
				n += 6
			} else {
				n += int64(utf8.RuneLen(r))
			}
		}
	}
	return n
}

// rawJSONSize matches the compact, HTML-escaped encoding of a valid RawMessage
// without constructing another parsed object graph or an encoded byte buffer.
func rawJSONSize(raw json.RawMessage) int64 {
	var n int64
	quoted := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if !quoted {
			if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
				continue
			}
			quoted = c == '"'
			n++
			continue
		}
		switch c {
		case '\\':
			n += 2
			i++ // Keep the existing JSON escape intact.
		case '"':
			quoted = false
			n++
		case '<', '>', '&':
			n += 6
		default:
			if c == 0xe2 && i+2 < len(raw) && raw[i+1] == 0x80 && (raw[i+2] == 0xa8 || raw[i+2] == 0xa9) {
				n += 6
				i += 2
			} else {
				n++
			}
		}
	}
	return n
}

type aggregateToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type aggregateTool struct {
	ID       string                `json:"id"`
	Type     string                `json:"type"`
	Function aggregateToolFunction `json:"function"`
}

type aggregateMessage struct {
	Role             string          `json:"role"`
	Content          string          `json:"content"`
	ReasoningContent string          `json:"reasoning_content,omitempty"`
	ToolCalls        []aggregateTool `json:"tool_calls,omitempty"`
}

type aggregateChoice struct {
	Index        int              `json:"index"`
	Message      aggregateMessage `json:"message"`
	FinishReason string           `json:"finish_reason"`
}

type aggregateResponse struct {
	ID      string            `json:"id"`
	Object  string            `json:"object"`
	Created int64             `json:"created"`
	Model   string            `json:"model"`
	Choices []aggregateChoice `json:"choices"`
	Usage   json.RawMessage   `json:"usage,omitempty"`
}
