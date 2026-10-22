package telemetry

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"
)

func TestCandidateStatsObservedAttemptsAndFallback(t *testing.T) {
	tm, err := Open(filepath.Join(t.TempDir(), "score.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tm.Close() })
	start := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	for _, e := range []Event{
		{RequestID: "r1", Provider: "a", Model: "m", Attempt: 1, Status: 503, LatencyMS: 100, FailureClass: "upstream_transient"},
		{RequestID: "r1", Provider: "a", Model: "m", Attempt: 2, Status: 200, LatencyMS: 20},
		{RequestID: "r2", Provider: "a", Model: "m", Attempt: 1, Status: 500, LatencyMS: 200},
		{RequestID: "r2", Provider: "b", Model: "m", Attempt: 1, Status: 200, LatencyMS: 50},
		{RequestID: "r3", Provider: "a", Model: "m", Attempt: 1, Status: 200, LatencyMS: 10},
		{RequestID: "r4", Provider: "a", Model: "m", Attempt: 1, Status: 200, Stream: true, LatencyMS: 1000},
		{Provider: "legacy", Model: "m", Status: 200},
		{RequestID: "cancel", Provider: "a", Model: "m", Attempt: 1, FailureClass: "canceled"},
	} {
		e.Ts = start
		tm.Record(e)
	}
	tm.Record(Event{Ts: start.Add(-time.Hour), RequestID: "outside", Provider: "a", Model: "m", Attempt: 1, Status: 200})
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tm.CandidateStats(context.Background(), start, start.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Rows != 6 || snapshot.Truncated || len(snapshot.Candidates) != 3 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	a := snapshot.Candidates[0]
	if a.Provider != "a" || a.Stream || a.Attempts != 4 || a.Requests != 3 || a.FirstAttempts != 3 || a.FirstAttemptSuccesses != 1 || a.FallbackRequests != 1 || a.P95LatencyMS != 200 {
		t.Fatalf("a=%+v", a)
	}
	if math.Abs(a.FirstAttemptSuccessRate-1.0/3) > 1e-9 || math.Abs(a.FallbackRate-1.0/3) > 1e-9 || a.Successes != 2 || a.MeanLatencyMS != 82.5 || a.SmoothedSuccessProbability != 0.5 || a.ExpectedCostMS != 165 {
		t.Fatalf("metrics=%+v", a)
	}
	if stream := snapshot.Candidates[1]; !stream.Stream || stream.P95LatencyMS != 1000 || stream.FallbackRate != 0 {
		t.Fatalf("stream=%+v", stream)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tm.CandidateStats(ctx, start, start.Add(time.Hour)); err == nil {
		t.Fatal("canceled query succeeded")
	}
}

func TestCandidateStatsUsesAllOutcomesInCost(t *testing.T) {
	tm, err := Open(filepath.Join(t.TempDir(), "cost.db"), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer tm.Close()
	for _, e := range []Event{
		{Provider: "a", Model: "m", RequestID: "one", Attempt: 1, Status: 500, LatencyMS: 1},
		{Provider: "a", Model: "m", RequestID: "two", Attempt: 1, Status: 200, LatencyMS: 99},
	} {
		e.Ts = time.Now()
		tm.Record(e)
	}
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tm.CandidateStats(context.Background(), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	a := snapshot.Candidates[0]
	if a.MeanLatencyMS != 50 || a.SmoothedSuccessProbability != 0.5 || a.ExpectedCostMS != 100 || a.P95LatencyMS != 99 {
		t.Fatalf("cost must include failed attempt: %+v", a)
	}
}
