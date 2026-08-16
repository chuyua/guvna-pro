// Package logring is a tiny fixed-size in-memory ring of recent log lines so
// the admin surface can tail logs without touching disk. The server process
// stays the source of truth; the ring only mirrors what was logged.
package logring

import (
	"io"
	"sync"
)

// Ring keeps the last size lines written through it.
type Ring struct {
	mu    sync.Mutex
	lines []string
	next  int
	size  int
}

func New(size int) *Ring {
	return &Ring{size: size, lines: make([]string, size)}
}

// Write appends to the ring and reports n so it satisfies io.Writer.
func (r *Ring) Write(p []byte) (int, error) {
	n := len(p)
	r.mu.Lock()
	defer r.mu.Unlock()
	for len(p) > 0 {
		i := indexByte(p, '\n')
		var line string
		if i < 0 {
			line = string(p)
			p = nil
		} else {
			line = string(p[:i])
			p = p[i+1:]
		}
		r.lines[r.next] = line
		r.next = (r.next + 1) % r.size
	}
	return n, nil
}

// Tail returns the last n lines in chronological order.
func (r *Ring) Tail(n int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n > r.size {
		n = r.size
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		idx := (r.next - n + i + r.size) % r.size
		if r.lines[idx] != "" {
			out = append(out, r.lines[idx])
		}
	}
	return out
}

// AsWriter returns an io.Writer that discards nothing and stores in the ring.
func (r *Ring) AsWriter() io.Writer { return r }

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}
