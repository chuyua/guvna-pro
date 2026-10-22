// Package telemetry records usage stats with an in-memory buffer flushed to
// SQLite on an interval. The hot path never touches disk: Record only appends
// to a slice under a mutex; a background goroutine drains the buffer in one
// transaction. A crash may lose events from the current interval.
package telemetry

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Event struct {
	Provider     string
	Chain        string
	Model        string
	Stream       bool
	Status       int
	TokensIn     int64
	TokensOut    int64
	Key          string
	Err          string
	Ts           time.Time
	RequestID    string
	Attempt      int
	LatencyMS    int64
	FailureClass string
}

// Telemetry owns a bounded FIFO and serializes database flushes. Record never
// waits for disk I/O. At most limit events are queued, plus one detached batch
// of at most limit events while Flush writes; total event storage is <= 2*limit.
type Telemetry struct {
	mu            sync.Mutex
	flushMu       sync.Mutex
	closeMu       sync.Mutex
	buffer        []Event
	head          int
	limit         int
	inflight      int
	flushesOK     uint64
	flushesFailed uint64
	retried       uint64
	dropped       uint64
	closing       bool
	closed        bool
	db            *sql.DB
	stop          chan struct{}
	stopOnce      sync.Once
	wg            sync.WaitGroup
}

const defaultBufferCap = 1 << 16

// A non-positive or malformed override keeps the default; the bound cannot be
// disabled accidentally. Open captures it once, outside the request hot path.
func bufferCap() int {
	n, err := strconv.Atoi(strings.TrimSpace(os.Getenv("GUVNA_TELEMETRY_BUFFER")))
	if err != nil || n <= 0 {
		return defaultBufferCap
	}
	return n
}

// Open creates the database (if needed), applies the schema, and starts the
// flusher goroutine. interval <= 0 disables background flushing; Close still
// flushes once.
func Open(path string, interval time.Duration) (*Telemetry, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS requests (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		ts TEXT NOT NULL,
		provider TEXT NOT NULL,
		chain TEXT NOT NULL,
		model TEXT NOT NULL,
		stream INTEGER NOT NULL,
		status INTEGER NOT NULL,
		tokens_in INTEGER NOT NULL DEFAULT 0,
		tokens_out INTEGER NOT NULL DEFAULT 0,
		key TEXT NOT NULL DEFAULT '',
		err TEXT NOT NULL DEFAULT '',
		request_id TEXT NOT NULL DEFAULT '',
		attempt INTEGER NOT NULL DEFAULT 0,
		latency_ms INTEGER NOT NULL DEFAULT 0,
		failure_class TEXT NOT NULL DEFAULT ''
	)`); err != nil {
		db.Close()
		return nil, err
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	t := &Telemetry{db: db, limit: bufferCap(), stop: make(chan struct{})}
	if interval > 0 {
		t.wg.Add(1)
		go t.flusher(interval)
	}
	return t, nil
}

// migrate adds columns introduced after the initial schema to existing
// databases (e.g. an existing production volume) without dropping data.
func migrate(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(requests)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	cols := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// Release the schema reader before ALTER, including with a single DB
	// connection. Existing rows keep the defaults of newly added columns.
	if err := rows.Close(); err != nil {
		return err
	}
	if !cols["key"] {
		if _, err := db.Exec(`ALTER TABLE requests ADD COLUMN key TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	for _, col := range []struct{ name, ddl string }{
		{"request_id", `ALTER TABLE requests ADD COLUMN request_id TEXT NOT NULL DEFAULT ''`},
		{"attempt", `ALTER TABLE requests ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0`},
		{"latency_ms", `ALTER TABLE requests ADD COLUMN latency_ms INTEGER NOT NULL DEFAULT 0`},
		{"failure_class", `ALTER TABLE requests ADD COLUMN failure_class TEXT NOT NULL DEFAULT ''`},
	} {
		if !cols[col.name] {
			if _, err := db.Exec(col.ddl); err != nil {
				return err
			}
		}
	}
	return nil
}

// Record adds an event, evicting the oldest queued event at capacity. Once
// shutdown begins, new events are rejected and included in dropped_events.
func (t *Telemetry) Record(e Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closing {
		t.dropped++
		return
	}
	if len(t.buffer) < t.limit {
		if len(t.buffer) == cap(t.buffer) {
			// Grow geometrically without allocating beyond the configured
			// bound; a detached batch and the queue each retain <= limit slots.
			next := make([]Event, len(t.buffer), min(t.limit, max(16, 2*cap(t.buffer))))
			copy(next, t.buffer)
			t.buffer = next
		}
		t.buffer = append(t.buffer, e)
		return
	}
	t.buffer[t.head] = e
	t.head = (t.head + 1) % len(t.buffer)
	t.dropped++
}

func (t *Telemetry) flusher(interval time.Duration) {
	defer t.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := t.Flush(); err != nil {
				log.Printf("telemetry flush: %v", err)
			}
		case <-t.stop:
			return
		}
	}
}

// Flush commits one FIFO batch. Concurrent Flush calls are serialized. A
// transaction failure restores the batch ahead of events recorded during I/O,
// evicting the oldest events if the combined queue exceeds its limit.
func (t *Telemetry) Flush() error {
	t.flushMu.Lock()
	defer t.flushMu.Unlock()
	return t.flush()
}

// flush requires flushMu. The deferred result handler covers Begin, Prepare,
// Exec and Commit failures identically.
func (t *Telemetry) flush() (err error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return sql.ErrConnDone
	}
	events, head := t.buffer, t.head
	t.buffer, t.head = nil, 0
	t.inflight = len(events)
	t.mu.Unlock()
	if len(events) == 0 {
		return nil
	}
	defer func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.inflight = 0
		if err == nil {
			t.flushesOK++
			return
		}
		t.flushesFailed++
		total := len(events) + len(t.buffer)
		drop := max(0, total-t.limit)
		t.dropped += uint64(drop)
		// retried_events counts failed-batch events retained for a later
		// attempt. Repeated failures count the retained events again.
		t.retried += uint64(max(0, len(events)-drop))
		merged := make([]Event, 0, min(total, t.limit))
		for i := drop; i < total; i++ {
			if i < len(events) {
				merged = append(merged, events[(head+i)%len(events)])
			} else {
				merged = append(merged, t.buffer[(t.head+i-len(events))%len(t.buffer)])
			}
		}
		t.buffer, t.head = merged, 0
	}()
	tx, err := t.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT INTO requests
		(ts, provider, chain, model, stream, status, tokens_in, tokens_out, key, err,
		 request_id, attempt, latency_ms, failure_class)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i := range events {
		e := events[(head+i)%len(events)]
		if _, err := stmt.Exec(e.Ts.UTC().Format(time.RFC3339Nano), e.Provider, e.Chain,
			e.Model, e.Stream, e.Status, e.TokensIn, e.TokensOut, e.Key, e.Err,
			e.RequestID, e.Attempt, e.LatencyMS, e.FailureClass); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Close stops background flushing and rejects new events before the final
// flush. It is safe to call concurrently or repeatedly. If that flush fails,
// the queue and database remain available: fix the storage failure, then call
// Flush or Close again. A successful Close releases the database handle.
func (t *Telemetry) Close() error {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closing = true
	t.mu.Unlock()
	t.stopOnce.Do(func() { close(t.stop) })
	t.wg.Wait()
	t.flushMu.Lock()
	defer t.flushMu.Unlock()
	if err := t.flush(); err != nil {
		return err
	}
	if err := t.db.Close(); err != nil {
		return err
	}
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	return nil
}

// DB exposes the underlying handle for read-only inspection.
func (t *Telemetry) DB() *sql.DB { return t.db }

// Stats is an aggregate usage snapshot for the admin surface.
type Stats struct {
	Requests   int            `json:"requests"`
	Streams    int            `json:"streams"`
	Errors     int            `json:"errors"`
	TokensIn   int64          `json:"tokens_in"`
	TokensOut  int64          `json:"tokens_out"`
	ByProvider []ProviderStat `json:"by_provider"`
}

// FlushStats is an in-memory lifecycle snapshot. Flush counts exclude empty
// batches; retried_events counts failed-batch events retained for retry (once
// per failed attempt); dropped_events counts queue evictions and shutdown
// rejections. buffer_len excludes the detached in-flight batch.
type FlushStats struct {
	FlushesOK      uint64 `json:"flushes_ok"`
	FlushesFailed  uint64 `json:"flushes_failed"`
	RetriedEvents  uint64 `json:"retried_events"`
	DroppedEvents  uint64 `json:"dropped_events"`
	BufferLen      int    `json:"buffer_len"`
	InflightEvents int    `json:"inflight_events"`
}

// FlushStats returns the current telemetry buffer counters.
func (t *Telemetry) FlushStats() FlushStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	return FlushStats{
		FlushesOK:      t.flushesOK,
		FlushesFailed:  t.flushesFailed,
		RetriedEvents:  t.retried,
		DroppedEvents:  t.dropped,
		BufferLen:      len(t.buffer),
		InflightEvents: t.inflight,
	}
}

type ProviderStat struct {
	Provider  string `json:"provider"`
	Requests  int    `json:"requests"`
	Errors    int    `json:"errors"`
	TokensIn  int64  `json:"tokens_in"`
	TokensOut int64  `json:"tokens_out"`
}

// Stats aggregates persisted events. Data reflects the last flush (up to the
// flush interval old) — good enough for observability.
func (t *Telemetry) Stats() (Stats, error) {
	var s Stats
	err := t.db.QueryRow(`SELECT COUNT(*),
		COALESCE(SUM(stream),0),
		COALESCE(SUM(CASE WHEN status < 200 OR status >= 300 THEN 1 ELSE 0 END),0),
		COALESCE(SUM(tokens_in),0),
		COALESCE(SUM(tokens_out),0)
		FROM requests`).Scan(&s.Requests, &s.Streams, &s.Errors, &s.TokensIn, &s.TokensOut)
	if err != nil {
		return s, err
	}
	rows, err := t.db.Query(`SELECT provider, COUNT(*),
		COALESCE(SUM(CASE WHEN status < 200 OR status >= 300 THEN 1 ELSE 0 END),0),
		COALESCE(SUM(tokens_in),0), COALESCE(SUM(tokens_out),0)
		FROM requests GROUP BY provider ORDER BY provider`)
	if err != nil {
		return s, err
	}
	defer rows.Close()
	for rows.Next() {
		var ps ProviderStat
		if err := rows.Scan(&ps.Provider, &ps.Requests, &ps.Errors, &ps.TokensIn, &ps.TokensOut); err != nil {
			return s, err
		}
		s.ByProvider = append(s.ByProvider, ps)
	}
	return s, rows.Err()
}

// usage is the minimal OpenAI usage object.
type usage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
}

// ParseUsage extracts token counts from an OpenAI chat completion body.
// Returns 0,0 when absent — counting is best-effort.
func ParseUsage(body []byte) (in, out int64) {
	var resp struct {
		Usage usage `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, 0
	}
	if resp.Usage.PromptTokens != 0 || resp.Usage.CompletionTokens != 0 {
		return resp.Usage.PromptTokens, resp.Usage.CompletionTokens
	}
	return 0, 0
}
