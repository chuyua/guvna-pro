package telemetry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func autoRequestEvent(id string, finished time.Time) AutoRequestEvent {
	return AutoRequestEvent{RequestID: id, StartedAt: finished.Add(-time.Second),
		FinishedAt: finished, Mode: "shadow", Policy: "static", DeciderSource: "fallback",
		Attempts: 1, Candidates: 1, Success: true, Status: 200, LatencyMS: 100}
}

func TestAutoRequestStatsActualPoliciesAndCancellation(t *testing.T) {
	tm := reliableTelemetry(t, 16)
	at := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	first := autoRequestEvent("first", at)
	first.LatencyMS = 80
	tm.RecordAutoRequest(first)
	// The queued value must not follow subsequent caller mutations.
	first.RequestID, first.Policy, first.Success = "changed", "telemetry", false
	keyRetry := autoRequestEvent("key-retry", at)
	keyRetry.Attempts, keyRetry.LatencyMS = 2, 120
	tm.RecordAutoRequest(keyRetry)
	fallback := autoRequestEvent("fallback", at)
	fallback.Attempts, fallback.Candidates, fallback.LatencyMS = 3, 2, 300
	tm.RecordAutoRequest(fallback)
	failure := autoRequestEvent("failure", at)
	failure.Success, failure.Status, failure.FailureClass, failure.LatencyMS = false, 503, "upstream_transient", 900
	tm.RecordAutoRequest(failure)
	canceled := autoRequestEvent("canceled", at)
	canceled.Success, canceled.Canceled, canceled.FailureClass = false, true, "canceled"
	canceled.Attempts, canceled.Candidates, canceled.LatencyMS = 4, 2, 9999
	tm.RecordAutoRequest(canceled)
	stream := autoRequestEvent("stream", at)
	stream.Stream, stream.LatencyMS = true, 3000
	tm.RecordAutoRequest(stream)
	selection := autoRequestEvent("selection", at)
	selection.Mode, selection.Policy, selection.DeciderSource = "decider", "none", ""
	selection.Success, selection.Attempts, selection.Candidates, selection.Status = false, 0, 0, 404
	tm.RecordAutoRequest(selection)
	alternative := autoRequestEvent("alternative", at)
	alternative.Mode, alternative.Policy = "telemetry", "telemetry"
	tm.RecordAutoRequest(alternative)
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tm.AutoRequestStats(context.Background(), at, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Rows != 8 || snapshot.Truncated || len(snapshot.Policies) != 4 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	selectionStat, regular, streamed, switched := snapshot.Policies[0], snapshot.Policies[1], snapshot.Policies[2], snapshot.Policies[3]
	if selectionStat.Policy != "none" || selectionStat.Requests != 1 || selectionStat.NoAttemptRequests != 1 || selectionStat.Failures != 1 || selectionStat.SuccessRate != 0 {
		t.Fatalf("selection=%+v", selectionStat)
	}
	if regular.Mode != "shadow" || regular.Policy != "static" || regular.Stream || regular.Requests != 5 ||
		regular.Attempts != 11 || regular.Successes != 3 || regular.Failures != 1 || regular.Canceled != 1 ||
		regular.FirstAttemptSuccesses != 1 || regular.FallbackRequests != 1 || regular.P95LatencyMS != 900 {
		t.Fatalf("actual request outcomes=%+v", regular)
	}
	if math.Abs(regular.FirstAttemptSuccessRate-0.25) > 1e-9 || math.Abs(regular.FallbackRate-0.25) > 1e-9 || math.Abs(regular.SuccessRate-0.75) > 1e-9 {
		t.Fatalf("cancellation entered rate denominator: %+v", regular)
	}
	if !streamed.Stream || streamed.P95LatencyMS != 3000 || switched.Mode != "telemetry" || switched.Policy != "telemetry" {
		t.Fatalf("groups mixed: %+v", snapshot.Policies)
	}
	var requestID, source, class string
	var status int
	if err := tm.db.QueryRow(`SELECT request_id, decider_source, status, failure_class FROM auto_requests WHERE request_id='first'`).
		Scan(&requestID, &source, &status, &class); err != nil {
		t.Fatal(err)
	}
	if requestID != "first" || source != "fallback" || status != 200 || class != "" {
		t.Fatalf("summary value changed: %s/%s/%d/%s", requestID, source, status, class)
	}
	var attempts int
	if err := tm.db.QueryRow("SELECT COUNT(*) FROM requests").Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("summaries leaked into attempt metrics: rows=%d err=%v", attempts, err)
	}
}

func TestAutoRequestStatsFinishWindowExactAndEmpty(t *testing.T) {
	tm := reliableTelemetry(t, 8)
	since := time.Date(2026, 10, 22, 12, 0, 0, 123456789, time.FixedZone("offset", 8*60*60))
	until := since.Add(3 * time.Nanosecond)
	for i, finished := range []time.Time{since.Add(-time.Nanosecond), since, until.Add(-time.Nanosecond), until} {
		e := autoRequestEvent(fmt.Sprint(i), finished)
		e.LatencyMS = int64(i + 1)
		tm.RecordAutoRequest(e)
	}
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tm.AutoRequestStats(context.Background(), since, until)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Rows != 2 || snapshot.Policies[0].P95LatencyMS != 3 {
		t.Fatalf("nanosecond completion window=%+v", snapshot)
	}
	empty, err := tm.AutoRequestStats(context.Background(), until.Add(time.Hour), until.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(empty.Policies)
	if err != nil || string(encoded) != "[]" || empty.Rows != 0 {
		t.Fatalf("empty snapshot=%+v json=%s err=%v", empty, encoded, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tm.AutoRequestStats(ctx, since, until); err == nil {
		t.Fatal("canceled query succeeded")
	}
}

func TestAutoRequestStatsP95AndAllCanceled(t *testing.T) {
	tm := reliableTelemetry(t, 32)
	at := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	for i := range 20 {
		e := autoRequestEvent(fmt.Sprint(i), at)
		e.LatencyMS = int64(i + 1)
		if i == 18 {
			e.Success, e.Status, e.FailureClass = false, 503, "upstream_transient"
		}
		tm.RecordAutoRequest(e)
	}
	e := autoRequestEvent("canceled", at)
	e.Mode, e.Policy, e.Canceled, e.Success, e.LatencyMS = "decider", "decider", true, false, 100000
	tm.RecordAutoRequest(e)
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := tm.AutoRequestStats(context.Background(), at, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if canceled := snapshot.Policies[0]; canceled.Requests != 1 || canceled.Canceled != 1 || canceled.Failures != 0 || canceled.SuccessRate != 0 || canceled.P95LatencyMS != 0 {
		t.Fatalf("all-canceled group=%+v", canceled)
	}
	if completed := snapshot.Policies[1]; completed.P95LatencyMS != 19 || completed.Failures != 1 {
		t.Fatalf("p95 must use nearest rank and include failed latency: %+v", completed)
	}
}

func TestAutoRequestMigrationPreservesLegacyAndReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE requests (
		id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, provider TEXT NOT NULL,
		chain TEXT NOT NULL, model TEXT NOT NULL, stream INTEGER NOT NULL,
		status INTEGER NOT NULL, tokens_in INTEGER NOT NULL DEFAULT 0,
		tokens_out INTEGER NOT NULL DEFAULT 0, err TEXT NOT NULL DEFAULT '')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO requests(ts,provider,chain,model,stream,status)
		VALUES('2026-01-01T00:00:00Z','old','main','m',0,200)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	tm, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 22, 0, 0, 0, 1, time.UTC)
	tm.RecordAutoRequest(autoRequestEvent("new", at))
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := migrate(reopened.db); err != nil {
		t.Fatalf("repeat migration=%v", err)
	}
	if got := persistedModels(t, reopened); !reflect.DeepEqual(got, []string{"m"}) {
		t.Fatalf("legacy attempts changed: %v", got)
	}
	snapshot, err := reopened.AutoRequestStats(context.Background(), at, at.Add(time.Nanosecond))
	if err != nil || snapshot.Rows != 1 || snapshot.Policies[0].Successes != 1 {
		t.Fatalf("reopened summary=%+v err=%v", snapshot, err)
	}
}

func TestAutoRequestMixedFlushRollbackAndRequeue(t *testing.T) {
	for _, stage := range []string{"prepare", "exec", "commit"} {
		t.Run(stage, func(t *testing.T) {
			tm := reliableTelemetry(t, 8)
			switch stage {
			case "prepare":
				execTelemetry(t, tm, "ALTER TABLE auto_requests RENAME TO saved_auto_requests")
				// Ordinary batches don't prepare the new statement.
				tm.Record(telemetryEvent(9))
				if err := tm.Flush(); err != nil {
					t.Fatalf("summary statement wasn't lazy: %v", err)
				}
				execTelemetry(t, tm, "DELETE FROM requests")
			case "exec":
				execTelemetry(t, tm, `CREATE TRIGGER fail_auto BEFORE INSERT ON auto_requests
					BEGIN SELECT RAISE(ABORT,'injected summary failure'); END`)
			case "commit":
				execTelemetry(t, tm, "PRAGMA foreign_keys=ON")
				execTelemetry(t, tm, "CREATE TABLE parent (id INTEGER PRIMARY KEY)")
				execTelemetry(t, tm, `CREATE TABLE child(parent_id INTEGER REFERENCES parent(id)
					DEFERRABLE INITIALLY DEFERRED)`)
				execTelemetry(t, tm, `CREATE TRIGGER fail_auto AFTER INSERT ON auto_requests
					BEGIN INSERT INTO child VALUES(1); END`)
			}
			tm.Record(telemetryEvent(0))
			tm.RecordAutoRequest(autoRequestEvent("summary", time.Now()))
			tm.Record(telemetryEvent(1))
			if err := tm.Flush(); err == nil {
				t.Fatal("injected failure succeeded")
			}
			if got := persistedModels(t, tm); len(got) != 0 {
				t.Fatalf("mixed transaction partially committed: %v", got)
			}
			if got := tm.FlushStats(); got.BufferLen != 3 || got.RetriedEvents != 3 || got.FlushesFailed != 1 || got.DroppedEvents != 0 {
				t.Fatalf("mixed batch recovery=%+v", got)
			}
			if stage == "prepare" {
				execTelemetry(t, tm, "ALTER TABLE saved_auto_requests RENAME TO auto_requests")
			} else {
				var summaries int
				if err := tm.db.QueryRow("SELECT COUNT(*) FROM auto_requests").Scan(&summaries); err != nil || summaries != 0 {
					t.Fatalf("summary partially committed: %d err=%v", summaries, err)
				}
				execTelemetry(t, tm, "DROP TRIGGER fail_auto")
			}
			if err := tm.Flush(); err != nil {
				t.Fatal(err)
			}
			if got := persistedModels(t, tm); !reflect.DeepEqual(got, []string{"0", "1"}) {
				t.Fatalf("attempt recovery=%v", got)
			}
			var summaries int
			if err := tm.db.QueryRow("SELECT COUNT(*) FROM auto_requests").Scan(&summaries); err != nil || summaries != 1 {
				t.Fatalf("summary duplicated/lost: %d err=%v", summaries, err)
			}
		})
	}
}

func TestAutoRequestMixedQueueOverflowKeepsNewest(t *testing.T) {
	tm := reliableTelemetry(t, 3)
	tm.Record(telemetryEvent(0))
	tm.RecordAutoRequest(autoRequestEvent("evicted", time.Now()))
	tm.Record(telemetryEvent(1))
	tm.RecordAutoRequest(autoRequestEvent("retained", time.Now()))
	tm.Record(telemetryEvent(2))
	if got := tm.FlushStats(); got.BufferLen != 3 || got.DroppedEvents != 2 {
		t.Fatalf("mixed overflow=%+v", got)
	}
	if err := tm.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := persistedModels(t, tm); !reflect.DeepEqual(got, []string{"1", "2"}) {
		t.Fatalf("wrong attempts retained: %v", got)
	}
	var id string
	if err := tm.db.QueryRow("SELECT request_id FROM auto_requests").Scan(&id); err != nil || id != "retained" {
		t.Fatalf("wrong summary retained: %s err=%v", id, err)
	}
}

func TestAutoRequestStatsCapReportsNewestSample(t *testing.T) {
	tm := reliableTelemetry(t, 1)
	finished := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	_, err := tm.db.Exec(`WITH RECURSIVE sequence(n) AS (
		SELECT 1 UNION ALL SELECT n+1 FROM sequence WHERE n < ?)
		INSERT INTO auto_requests(request_id,started_at,finished_at,mode,policy,
		decider_source,stream,attempts,candidates,success,canceled,status,failure_class,latency_ms)
		SELECT CAST(n AS TEXT), ?, ?, 'shadow', CASE WHEN n=1 THEN 'oldest' ELSE 'static' END,
		'fallback',0,1,1,1,0,200,'',1 FROM sequence`,
		scoringRowLimit+1, finished.Add(-time.Second).Format(autoRequestTimeLayout), finished.Format(autoRequestTimeLayout))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tm.AutoRequestStats(context.Background(), finished, finished.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Truncated || snapshot.Rows != scoringRowLimit || len(snapshot.Policies) != 1 || snapshot.Policies[0].Policy != "static" || snapshot.Policies[0].Requests != scoringRowLimit {
		t.Fatalf("newest bounded sample=%+v", snapshot)
	}
}
