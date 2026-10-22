package telemetry

import (
	"context"
	"database/sql"
	"sort"
	"time"
)

// AutoRequestEvent is the completed outcome of one actual Auto request. It does
// not describe the result of an unexecuted shadow order.
type AutoRequestEvent struct {
	RequestID     string    `json:"request_id"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	Mode          string    `json:"mode"`
	Policy        string    `json:"policy"`
	DeciderSource string    `json:"decider_source"`
	Stream        bool      `json:"stream"`
	Attempts      int       `json:"attempts"`
	Candidates    int       `json:"candidates"`
	Success       bool      `json:"success"`
	Canceled      bool      `json:"canceled"`
	Status        int       `json:"status"`
	FailureClass  string    `json:"failure_class"`
	LatencyMS     int64     `json:"latency_ms"`
}

// RecordAutoRequest queues a value copy in the same bounded FIFO as attempts.
// Storage failures and queue evictions use the existing flush/drop counters.
func (t *Telemetry) RecordAutoRequest(e AutoRequestEvent) {
	t.Record(Event{AutoRequest: &e})
}

// Fixed precision keeps SQLite text comparisons exact at nanosecond boundaries.
const autoRequestTimeLayout = "2006-01-02T15:04:05.000000000Z"

func createAutoRequestSchema(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS auto_requests (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		request_id TEXT NOT NULL,
		started_at TEXT NOT NULL,
		finished_at TEXT NOT NULL,
		mode TEXT NOT NULL,
		policy TEXT NOT NULL,
		decider_source TEXT NOT NULL,
		stream INTEGER NOT NULL,
		attempts INTEGER NOT NULL,
		candidates INTEGER NOT NULL,
		success INTEGER NOT NULL,
		canceled INTEGER NOT NULL,
		status INTEGER NOT NULL,
		failure_class TEXT NOT NULL,
		latency_ms INTEGER NOT NULL
	)`); err != nil {
		return err
	}
	_, err := db.Exec(`CREATE INDEX IF NOT EXISTS auto_requests_finished_at
		ON auto_requests(finished_at, id)`)
	return err
}

const insertAutoRequestSQL = `INSERT INTO auto_requests
	(request_id, started_at, finished_at, mode, policy, decider_source, stream,
	 attempts, candidates, success, canceled, status, failure_class, latency_ms)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

func insertAutoRequest(stmt *sql.Stmt, e AutoRequestEvent) error {
	_, err := stmt.Exec(e.RequestID, e.StartedAt.UTC().Format(autoRequestTimeLayout),
		e.FinishedAt.UTC().Format(autoRequestTimeLayout), e.Mode, e.Policy,
		e.DeciderSource, e.Stream, e.Attempts, e.Candidates, e.Success, e.Canceled,
		e.Status, e.FailureClass, max(0, e.LatencyMS))
	return err
}

// AutoRequestStat groups completed requests by configured mode and actual policy.
// Cancellation is visible in counts but excluded from rates and latency.
type AutoRequestStat struct {
	Mode                    string  `json:"mode"`
	Policy                  string  `json:"policy"`
	Stream                  bool    `json:"stream"`
	Requests                int     `json:"requests"`
	Successes               int     `json:"successes"`
	Failures                int     `json:"failures"`
	Canceled                int     `json:"canceled"`
	Attempts                int     `json:"attempts"`
	NoAttemptRequests       int     `json:"no_attempt_requests"`
	FirstAttemptSuccesses   int     `json:"first_attempt_successes"`
	FallbackRequests        int     `json:"fallback_requests"`
	FirstAttemptSuccessRate float64 `json:"first_attempt_success_rate"`
	FallbackRate            float64 `json:"fallback_rate"`
	SuccessRate             float64 `json:"success_rate"`
	P95LatencyMS            int64   `json:"p95_latency_ms"`
}

type AutoRequestSnapshot struct {
	Since     time.Time         `json:"since"`
	Until     time.Time         `json:"until"`
	Rows      int               `json:"rows"`
	Truncated bool              `json:"truncated"`
	Policies  []AutoRequestStat `json:"policies"`
}

// AutoRequestStats reads complete summaries whose finish time is in
// [since,until). A request can start before since and still have its whole
// outcome included; unlike an attempt window, the request is never reconstructed
// from partial rows. Results describe actual traffic, not comparable experiments.
func (t *Telemetry) AutoRequestStats(ctx context.Context, since, until time.Time) (AutoRequestSnapshot, error) {
	snapshot := AutoRequestSnapshot{Since: since, Until: until, Policies: []AutoRequestStat{}}
	rows, err := t.db.QueryContext(ctx, `SELECT mode, policy, stream, attempts,
		candidates, success, canceled, latency_ms FROM auto_requests
		WHERE finished_at >= ? AND finished_at < ?
		ORDER BY finished_at DESC, id DESC LIMIT ?`,
		since.UTC().Format(autoRequestTimeLayout), until.UTC().Format(autoRequestTimeLayout), scoringRowLimit+1)
	if err != nil {
		return snapshot, err
	}
	defer rows.Close()
	type key struct {
		mode, policy string
		stream       bool
	}
	type aggregate struct {
		stat      AutoRequestStat
		latencies []int64
	}
	groups := make(map[key]*aggregate)
	for rows.Next() {
		if snapshot.Rows == scoringRowLimit {
			snapshot.Truncated = true
			break
		}
		var k key
		var attempts, candidates int
		var success, canceled bool
		var latency int64
		if err := rows.Scan(&k.mode, &k.policy, &k.stream, &attempts,
			&candidates, &success, &canceled, &latency); err != nil {
			return snapshot, err
		}
		snapshot.Rows++
		a := groups[k]
		if a == nil {
			a = &aggregate{stat: AutoRequestStat{Mode: k.mode, Policy: k.policy, Stream: k.stream}}
			groups[k] = a
		}
		a.stat.Requests++
		a.stat.Attempts += attempts
		if attempts == 0 {
			a.stat.NoAttemptRequests++
		}
		if canceled {
			a.stat.Canceled++
			continue
		}
		if success {
			a.stat.Successes++
			if attempts == 1 {
				a.stat.FirstAttemptSuccesses++
			}
		} else {
			a.stat.Failures++
		}
		if candidates > 1 {
			a.stat.FallbackRequests++
		}
		a.latencies = append(a.latencies, max(0, latency))
	}
	if err := rows.Err(); err != nil {
		return snapshot, err
	}
	for _, a := range groups {
		if completed := a.stat.Requests - a.stat.Canceled; completed > 0 {
			a.stat.FirstAttemptSuccessRate = float64(a.stat.FirstAttemptSuccesses) / float64(completed)
			a.stat.FallbackRate = float64(a.stat.FallbackRequests) / float64(completed)
			a.stat.SuccessRate = float64(a.stat.Successes) / float64(completed)
			sort.Slice(a.latencies, func(i, j int) bool { return a.latencies[i] < a.latencies[j] })
			a.stat.P95LatencyMS = a.latencies[(95*len(a.latencies)+99)/100-1]
		}
		snapshot.Policies = append(snapshot.Policies, a.stat)
	}
	sort.Slice(snapshot.Policies, func(i, j int) bool {
		a, b := snapshot.Policies[i], snapshot.Policies[j]
		if a.Mode != b.Mode {
			return a.Mode < b.Mode
		}
		if a.Policy != b.Policy {
			return a.Policy < b.Policy
		}
		return !a.Stream && b.Stream
	})
	return snapshot, nil
}
