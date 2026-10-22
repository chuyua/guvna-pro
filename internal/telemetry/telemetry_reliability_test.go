package telemetry

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func reliableTelemetry(t *testing.T, limit int) *Telemetry {
	t.Helper()
	t.Setenv("GUVNA_TELEMETRY_BUFFER", fmt.Sprint(limit))
	tm, err := Open(filepath.Join(t.TempDir(), "t.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	tm.db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := tm.Close(); err != nil {
			t.Error(err)
		}
	})
	return tm
}

func execTelemetry(t *testing.T, tm *Telemetry, query string) {
	t.Helper()
	if _, err := tm.db.Exec(query); err != nil {
		t.Fatal(err)
	}
}

func telemetryEvent(i int) Event {
	return Event{Provider: "p", Chain: "main", Model: fmt.Sprint(i), Status: 200,
		RequestID: "request", Attempt: i + 1, LatencyMS: int64(i),
		Ts: time.Date(2026, 10, 21, 12, 0, 0, 123456789, time.UTC)}
}

func persistedModels(t *testing.T, tm *Telemetry) []string {
	t.Helper()
	rows, err := tm.db.Query("SELECT model FROM requests ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var models []string
	for rows.Next() {
		var model string
		if err := rows.Scan(&model); err != nil {
			t.Fatal(err)
		}
		models = append(models, model)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return models
}

func TestFlushFailuresRollbackAndRecover(t *testing.T) {
	for _, stage := range []string{"begin", "prepare", "exec", "commit"} {
		t.Run(stage, func(t *testing.T) {
			tm := reliableTelemetry(t, 8)
			var path string
			if err := tm.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
				t.Fatal(err)
			}
			switch stage {
			case "begin":
				// Open a replacement connection after the failure; this exercises
				// the real database/sql Begin failure without a production wrapper.
				if err := tm.db.Close(); err != nil {
					t.Fatal(err)
				}
			case "prepare":
				execTelemetry(t, tm, "ALTER TABLE requests RENAME TO saved_requests")
			case "exec":
				execTelemetry(t, tm, `CREATE TRIGGER fail_event BEFORE INSERT ON requests
					WHEN NEW.model='1' BEGIN SELECT RAISE(ABORT,'injected insert failure'); END`)
			case "commit":
				execTelemetry(t, tm, "PRAGMA foreign_keys=ON")
				execTelemetry(t, tm, "CREATE TABLE parent (id INTEGER PRIMARY KEY)")
				execTelemetry(t, tm, `CREATE TABLE child (parent_id INTEGER REFERENCES parent(id)
					DEFERRABLE INITIALLY DEFERRED)`)
				execTelemetry(t, tm, `CREATE TRIGGER fail_commit AFTER INSERT ON requests
					BEGIN INSERT INTO child VALUES(1); END`)
			}
			tm.Record(telemetryEvent(0))
			tm.Record(telemetryEvent(1))
			if err := tm.Flush(); err == nil {
				t.Fatal("injected failure succeeded")
			}
			if got := tm.FlushStats(); got.FlushesFailed != 1 || got.RetriedEvents != 2 ||
				got.BufferLen != 2 || got.InflightEvents != 0 || got.DroppedEvents != 0 {
				t.Fatalf("failed flush counters: %+v", got)
			}
			switch stage {
			case "begin":
				var err error
				tm.db, err = sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				tm.db.SetMaxOpenConns(1)
			case "prepare":
				execTelemetry(t, tm, "ALTER TABLE saved_requests RENAME TO requests")
			case "exec":
				if got := persistedModels(t, tm); len(got) != 0 {
					t.Fatalf("partial transaction persisted: %v", got)
				}
				execTelemetry(t, tm, "DROP TRIGGER fail_event")
			case "commit":
				if got := persistedModels(t, tm); len(got) != 0 {
					t.Fatalf("failed commit persisted: %v", got)
				}
				execTelemetry(t, tm, "DROP TRIGGER fail_commit")
			}
			tm.Record(telemetryEvent(2))
			if err := tm.Flush(); err != nil {
				t.Fatal(err)
			}
			if got := persistedModels(t, tm); !reflect.DeepEqual(got, []string{"0", "1", "2"}) {
				t.Fatalf("recovered FIFO: %v", got)
			}
			if got := tm.FlushStats(); got.FlushesOK != 1 || got.FlushesFailed != 1 ||
				got.RetriedEvents != 2 || got.BufferLen != 0 {
				t.Fatalf("recovery counters: %+v", got)
			}
		})
	}
}

func TestRepeatedFlushFailureKeepsBufferBounded(t *testing.T) {
	tm := reliableTelemetry(t, 4)
	execTelemetry(t, tm, `CREATE TRIGGER fail_event BEFORE INSERT ON requests
		BEGIN SELECT RAISE(ABORT,'persistent failure'); END`)
	for failure := range 20 {
		for i := range 3 {
			tm.Record(telemetryEvent(failure*3 + i))
		}
		if err := tm.Flush(); err == nil {
			t.Fatal("failure injection did not fail")
		}
		stats := tm.FlushStats()
		if stats.BufferLen > 4 || cap(tm.buffer) > 4 || stats.InflightEvents != 0 {
			t.Fatalf("buffer exceeded bound: %+v capacity=%d", stats, cap(tm.buffer))
		}
	}
	stats := tm.FlushStats()
	if stats.FlushesFailed != 20 || stats.DroppedEvents != 56 || stats.RetriedEvents != 79 {
		t.Fatalf("persistent failure counters: %+v", stats)
	}
	execTelemetry(t, tm, "DROP TRIGGER fail_event")
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := persistedModels(t, tm); !reflect.DeepEqual(got, []string{"56", "57", "58", "59"}) {
		t.Fatalf("newest events were not retained: %v", got)
	}
}

func TestBufferCapKeepsNewestWithoutOverflowAllocations(t *testing.T) {
	tm := reliableTelemetry(t, 4)
	for i := range 7 {
		tm.Record(telemetryEvent(i))
	}
	if got := tm.FlushStats(); got.BufferLen != 4 || got.DroppedEvents != 3 {
		t.Fatalf("overflow counters: %+v", got)
	}
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := persistedModels(t, tm); !reflect.DeepEqual(got, []string{"3", "4", "5", "6"}) {
		t.Fatalf("oldest were not evicted: %v", got)
	}
	for range 4 {
		tm.Record(telemetryEvent(7))
	}
	e := telemetryEvent(8)
	if allocs := testing.AllocsPerRun(100, func() { tm.Record(e) }); allocs != 0 {
		t.Fatalf("Record at capacity allocates %f times", allocs)
	}
	// Environment changes do not alter a running queue's configured bound.
	t.Setenv("GUVNA_TELEMETRY_BUFFER", "100")
	for range 10 {
		tm.Record(e)
	}
	if got := tm.FlushStats().BufferLen; got != 4 {
		t.Fatalf("cap drifted: %d", got)
	}
}

func TestBufferCapRejectsNonPositiveAndInvalidOverrides(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "invalid", "999999999999999999999999"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("GUVNA_TELEMETRY_BUFFER", value)
			if got := bufferCap(); got != defaultBufferCap {
				t.Fatalf("cap=%d", got)
			}
		})
	}
	t.Setenv("GUVNA_TELEMETRY_BUFFER", " 7 ")
	if got := bufferCap(); got != 7 {
		t.Fatalf("cap=%d", got)
	}
}

func awaitInflight(t *testing.T, tm *Telemetry, want int) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if tm.FlushStats().InflightEvents == want {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("flush never detached its batch")
		}
	}
}

func TestFailedMergeBoundsBufferWithConcurrentRecord(t *testing.T) {
	tm := reliableTelemetry(t, 4)
	execTelemetry(t, tm, "ALTER TABLE requests RENAME TO saved_requests")
	// Holding the only connection pauses Begin after Flush has detached its
	// batch, while leaving Record independent of disk access.
	conn, err := tm.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for i := range 6 {
		tm.Record(telemetryEvent(i))
	}
	done := make(chan error, 1)
	go func() { done <- tm.Flush() }()
	awaitInflight(t, tm, 4)
	for i := 6; i < 8; i++ {
		tm.Record(telemetryEvent(i))
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("missing-table flush succeeded")
	}
	if got := tm.FlushStats(); got.BufferLen != 4 || got.DroppedEvents != 4 ||
		got.RetriedEvents != 2 || got.InflightEvents != 0 {
		t.Fatalf("merge counters: %+v", got)
	}
	execTelemetry(t, tm, "ALTER TABLE saved_requests RENAME TO requests")
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := persistedModels(t, tm); !reflect.DeepEqual(got, []string{"4", "5", "6", "7"}) {
		t.Fatalf("merge FIFO: %v", got)
	}
}

func TestConcurrentFlushesPreserveBatchOrder(t *testing.T) {
	tm := reliableTelemetry(t, 64)
	conn, err := tm.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for i := range 32 {
		tm.Record(telemetryEvent(i))
	}
	done := make(chan error, 2)
	go func() { done <- tm.Flush() }()
	awaitInflight(t, tm, 32)
	for i := 32; i < 64; i++ {
		tm.Record(telemetryEvent(i))
	}
	go func() { done <- tm.Flush() }()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	got := persistedModels(t, tm)
	want := make([]string, 64)
	for i := range want {
		want[i] = fmt.Sprint(i)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("flush ordering changed: %v", got)
	}
	if stats := tm.FlushStats(); stats.FlushesOK != 2 || stats.BufferLen != 0 {
		t.Fatalf("concurrent counters: %+v", stats)
	}
}

func TestConcurrentRecordAndFlushDoNotLoseEvents(t *testing.T) {
	tm := reliableTelemetry(t, 1000)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			for i := range 100 {
				tm.Record(telemetryEvent(worker*100 + i))
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			for range 10 {
				if err := tm.Flush(); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	got := persistedModels(t, tm)
	seen := make(map[string]bool)
	for _, model := range got {
		if seen[model] {
			t.Fatalf("duplicate event %s", model)
		}
		seen[model] = true
	}
	if len(got) != 800 || tm.FlushStats().DroppedEvents != 0 {
		t.Fatalf("lost events: rows=%d stats=%+v", len(got), tm.FlushStats())
	}
}

func TestCloseFailureRetainsQueueAndCanRetry(t *testing.T) {
	tm := reliableTelemetry(t, 4)
	execTelemetry(t, tm, `CREATE TRIGGER fail_event BEFORE INSERT ON requests
		BEGIN SELECT RAISE(ABORT,'injected close failure'); END`)
	tm.Record(telemetryEvent(0))
	if err := tm.Close(); err == nil {
		t.Fatal("Close hid flush failure")
	}
	if got := tm.FlushStats(); got.BufferLen != 1 {
		t.Fatalf("Close lost event: %+v", got)
	}
	execTelemetry(t, tm, "DROP TRIGGER fail_event")
	tm.Record(telemetryEvent(1)) // shutdown rejects rather than silently leaks
	if got := tm.FlushStats(); got.DroppedEvents != 1 {
		t.Fatalf("shutdown rejection: %+v", got)
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tm.db.Ping(); err == nil {
		t.Fatal("successful Close leaked database")
	}
	if got := tm.FlushStats(); got.BufferLen != 0 || got.FlushesOK != 1 {
		t.Fatalf("close retry counters: %+v", got)
	}
}

func TestConcurrentCloseIsIdempotent(t *testing.T) {
	tm := reliableTelemetry(t, 4)
	tm.Record(telemetryEvent(0))
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := tm.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestMigrationPreservesLegacyRowsAndAttemptColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE requests (
		id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
		provider TEXT NOT NULL, chain TEXT NOT NULL, model TEXT NOT NULL,
		stream INTEGER NOT NULL, status INTEGER NOT NULL,
		tokens_in INTEGER NOT NULL DEFAULT 0, tokens_out INTEGER NOT NULL DEFAULT 0,
		err TEXT NOT NULL DEFAULT '')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO requests (ts,provider,chain,model,stream,status)
		VALUES ('2026-01-01T00:00:00Z','old','main','m',0,200)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	tm, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()
	e := telemetryEvent(1)
	e.FailureClass = "rate_limit"
	tm.Record(e)
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	var id, fc, timestamp string
	var attempt int
	var latency int64
	if err := tm.db.QueryRow(`SELECT request_id,attempt,latency_ms,failure_class,ts
		FROM requests WHERE provider='p'`).Scan(&id, &attempt, &latency, &fc, &timestamp); err != nil {
		t.Fatal(err)
	}
	if id != e.RequestID || attempt != e.Attempt || latency != e.LatencyMS ||
		fc != e.FailureClass || timestamp != e.Ts.Format(time.RFC3339Nano) {
		t.Fatalf("attempt columns changed: %s/%d/%d/%s/%s", id, attempt, latency, fc, timestamp)
	}
	if err := tm.db.QueryRow(`SELECT request_id,attempt,latency_ms,failure_class FROM requests
		WHERE provider='old'`).Scan(&id, &attempt, &latency, &fc); err != nil {
		t.Fatal(err)
	}
	if id != "" || attempt != 0 || latency != 0 || fc != "" {
		t.Fatal("legacy defaults changed")
	}
	if err := migrate(tm.db); err != nil {
		t.Fatalf("repeat migration: %v", err)
	}
}
