package server

import (
	"bufio"
	"context"
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
