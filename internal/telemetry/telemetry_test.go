package telemetry

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRecordAndFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	tm, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	tm.Record(Event{Provider: "groq", Chain: "fast", Model: "llama-3.3-70b-versatile",
		Stream: false, Status: 200, TokensIn: 10, TokensOut: 20, Ts: time.Now()})
	tm.Record(Event{Provider: "gemini", Chain: "smart", Model: "gemini-2.5-flash",
		Stream: true, Status: 429, Err: "rate limited", Ts: time.Now()})
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := tm.db.QueryRow(`SELECT COUNT(*) FROM requests`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("rows = %d, want 2", n)
	}
	var errCol string
	if err := tm.db.QueryRow(`SELECT err FROM requests WHERE provider='gemini'`).Scan(&errCol); err != nil {
		t.Fatal(err)
	}
	if errCol != "rate limited" {
		t.Errorf("err col = %q", errCol)
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyFlushIsNoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	tm, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := tm.Flush(); err != nil {
		t.Fatalf("flush of empty buffer: %v", err)
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParseUsage(t *testing.T) {
	in, out := ParseUsage([]byte(`{"choices":[{}],"usage":{"prompt_tokens":42,"completion_tokens":7}}`))
	if in != 42 || out != 7 {
		t.Errorf("got %d/%d, want 42/7", in, out)
	}
	in, out = ParseUsage([]byte(`{"choices":[{}]}`))
	if in != 0 || out != 0 {
		t.Errorf("absent usage: got %d/%d", in, out)
	}
	in, out = ParseUsage([]byte(`not json`))
	if in != 0 || out != 0 {
		t.Errorf("bad json: got %d/%d", in, out)
	}
}

func TestCloseFlushesRemaining(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	tm, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	tm.Record(Event{Provider: "bazaarlink", Chain: "fallback", Model: "qwen3.7-flash:free",
		Status: 200, Ts: time.Now()})
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen read-only to count rows.
	tm2, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tm2.db.QueryRow(`SELECT COUNT(*) FROM requests`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("rows = %d, want 1", n)
	}
	tm2.Close()
}
