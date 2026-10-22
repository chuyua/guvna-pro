package telemetry

import (
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openProviderHealthTest(t *testing.T) *Telemetry {
	t.Helper()
	tm, err := Open(filepath.Join(t.TempDir(), "provider-health.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tm.Close() })
	return tm
}

func TestProviderHealthOutcomeClassesAndStreams(t *testing.T) {
	tm := openProviderHealthTest(t)
	start := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	for i, e := range []Event{
		{Status: 200, LatencyMS: 10},
		{Status: 502, FailureClass: "upstream_transient", LatencyMS: 200},
		{FailureClass: "local_network", LatencyMS: 300},
		{Status: 401, FailureClass: "auth", LatencyMS: 40},
		{Status: 429, FailureClass: "rate_limit", LatencyMS: 50},
		{Status: 404, FailureClass: "caller_error", LatencyMS: 60},
		{Status: 400, FailureClass: "model_incompatible", LatencyMS: 70},
		{Status: 200, FailureClass: "canceled", LatencyMS: 9000},
		{Status: 502, FailureClass: "future_class", LatencyMS: 80},
		{Status: 503, LatencyMS: 90},
		{Status: 200, Stream: true, LatencyMS: 1000},
	} {
		e.Ts, e.Provider, e.Model = start.Add(time.Duration(i)*time.Millisecond), "a", "m"
		e.RequestID, e.Attempt = "request", i+1
		tm.Record(e)
	}
	// Old usage-only rows and incomplete attempt metadata do not provide evidence.
	tm.Record(Event{Ts: start, Provider: "legacy", Status: 200})
	tm.Record(Event{Ts: start, Provider: "legacy", Attempt: 1, Status: 200})
	tm.Record(Event{Ts: start, Provider: "legacy", RequestID: "legacy", Status: 200})
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tm.ProviderHealthStats(context.Background(), start, start.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Rows != 11 || snapshot.Truncated || len(snapshot.Providers) != 2 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	a := snapshot.Providers[0]
	if a.Provider != "a" || a.Stream || a.Attempts != 10 || a.Successes != 1 || a.UpstreamFailures != 1 || a.NetworkFailures != 1 || a.AuthFailures != 1 || a.RateLimited != 1 || a.RequestErrors != 2 || a.Canceled != 1 || a.OtherFailures != 2 {
		t.Fatalf("class partition=%+v", a)
	}
	if total := a.Successes + a.UpstreamFailures + a.NetworkFailures + a.AuthFailures + a.RateLimited + a.RequestErrors + a.Canceled + a.OtherFailures; total != a.Attempts {
		t.Fatalf("partition totals %d, attempts %d", total, a.Attempts)
	}
	if a.HealthSamples != 4 || a.ServiceSuccessRate == nil || math.Abs(*a.ServiceSuccessRate-0.25) > 1e-9 || a.P95LatencyMS != 300 || !a.LastAttemptAt.Equal(start.Add(9*time.Millisecond)) {
		t.Fatalf("availability and latency=%+v", a)
	}
	stream := snapshot.Providers[1]
	if !stream.Stream || stream.Attempts != 1 || stream.HealthSamples != 1 || stream.ServiceSuccessRate == nil || *stream.ServiceSuccessRate != 1 || stream.P95LatencyMS != 1000 {
		t.Fatalf("stream=%+v", stream)
	}
}

func TestProviderHealthNoDataAndNeutralOutcomes(t *testing.T) {
	tm := openProviderHealthTest(t)
	start := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	snapshot, err := tm.ProviderHealthStats(context.Background(), start, start.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Rows != 0 || snapshot.Truncated || !strings.Contains(string(encoded), `"providers":[]`) {
		t.Fatalf("empty snapshot=%s", encoded)
	}
	for _, e := range []Event{
		{Provider: "neutral", FailureClass: "auth", Status: 403, LatencyMS: -100},
		{Provider: "neutral", FailureClass: "caller_error", Status: 404, LatencyMS: 10},
		{Provider: "neutral", FailureClass: "canceled", LatencyMS: 100000},
		{Provider: "only-cancel", FailureClass: "canceled", LatencyMS: 100000},
	} {
		e.RequestID, e.Attempt, e.Ts = "r", 1, start
		tm.Record(e)
	}
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot, err = tm.ProviderHealthStats(context.Background(), start, start.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Providers) != 2 {
		t.Fatalf("providers=%+v", snapshot.Providers)
	}
	neutral, canceled := snapshot.Providers[0], snapshot.Providers[1]
	if neutral.Provider != "neutral" || neutral.HealthSamples != 0 || neutral.ServiceSuccessRate != nil || neutral.P95LatencyMS != 10 || neutral.Attempts != 3 || neutral.Canceled != 1 {
		t.Fatalf("neutral=%+v", neutral)
	}
	if canceled.Provider != "only-cancel" || canceled.P95LatencyMS != 0 || canceled.ServiceSuccessRate != nil || canceled.Canceled != 1 {
		t.Fatalf("canceled=%+v", canceled)
	}
	encoded, err = json.Marshal(neutral)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"service_success_rate":null`) {
		t.Fatalf("no availability evidence must remain null: %s", encoded)
	}
}

func TestProviderHealthExactWindowAndTimestampPrecision(t *testing.T) {
	tm := openProviderHealthTest(t)
	start := time.Date(2026, 10, 21, 0, 0, 0, 123456789, time.UTC)
	until := start.Add(2 * time.Nanosecond)
	for _, at := range []time.Time{start.Add(-time.Nanosecond), start, start.Add(time.Nanosecond), until, until.Add(time.Nanosecond)} {
		tm.Record(Event{Ts: at, Provider: "nanoseconds", RequestID: "r", Attempt: 1, Status: 200})
	}
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tm.ProviderHealthStats(context.Background(), start, until)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Rows != 2 || snapshot.Truncated || len(snapshot.Providers) != 1 || !snapshot.Providers[0].LastAttemptAt.Equal(start.Add(time.Nanosecond)) {
		t.Fatalf("nanosecond bounds=%+v", snapshot)
	}
	// The old attempt timestamp column has variable RFC3339 precision. UTC
	// offsets also describe the same instant and must not be compared as text.
	base := start.Truncate(time.Second)
	for _, raw := range []string{base.Format(time.RFC3339Nano), base.Format("2006-01-02T15:04:05.000Z"), base.In(time.FixedZone("offset", 8*60*60)).Format(time.RFC3339Nano)} {
		if _, err := tm.db.Exec(`INSERT INTO requests(ts,provider,chain,model,stream,status,request_id,attempt)
			VALUES (?, 'precision', '', 'm', 0, 200, 'precision', 1)`, raw); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err = tm.ProviderHealthStats(context.Background(), base, base.Add(time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Rows != 3 || snapshot.Truncated || len(snapshot.Providers) != 1 || snapshot.Providers[0].Provider != "precision" || !snapshot.Providers[0].LastAttemptAt.Equal(base) {
		t.Fatalf("variable precision=%+v", snapshot)
	}
}

func TestProviderHealthBoundedNewestRows(t *testing.T) {
	tm := openProviderHealthTest(t)
	start := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	_, err := tm.db.Exec(`WITH RECURSIVE sequence(n) AS (
		SELECT 1 UNION ALL SELECT n+1 FROM sequence WHERE n < ?)
		INSERT INTO requests(ts,provider,chain,model,stream,status,request_id,attempt,latency_ms)
		SELECT ?, CASE WHEN n=1 THEN 'oldest' ELSE 'current' END, '', 'm', 0, 200,
		CAST(n AS TEXT), 1, 1 FROM sequence`, scoringRowLimit+1, start.Add(2*time.Nanosecond).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tm.ProviderHealthStats(context.Background(), start, start.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Truncated || snapshot.Rows != scoringRowLimit || len(snapshot.Providers) != 1 || snapshot.Providers[0].Provider != "current" || snapshot.Providers[0].Attempts != scoringRowLimit {
		t.Fatalf("newest bounded sample=%+v", snapshot)
	}
	// Many near-boundary rows still cannot cause unbounded scanning. The exact
	// window has no data, but the scan cap is reported rather than hidden.
	snapshot, err = tm.ProviderHealthStats(context.Background(), start, start.Add(time.Nanosecond))
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Truncated || snapshot.Rows != 0 || len(snapshot.Providers) != 0 {
		t.Fatalf("bounded exact filtering=%+v", snapshot)
	}
}

func TestProviderHealthQueryErrorsAreVisible(t *testing.T) {
	tm := openProviderHealthTest(t)
	start := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tm.ProviderHealthStats(ctx, start, start.Add(time.Hour)); err == nil {
		t.Fatal("canceled query succeeded")
	}
	// A SQLite-compatible but invalid writer timestamp must not turn into a
	// successful empty/zero report.
	if _, err := tm.db.Exec(`INSERT INTO requests(ts,provider,chain,model,stream,status,request_id,attempt)
		VALUES ('2026-10-21 00:00:00', 'broken', '', 'm', 0, 200, 'broken', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tm.ProviderHealthStats(context.Background(), start, start.Add(time.Hour)); err == nil {
		t.Fatal("malformed timestamp silently accepted")
	}
}
