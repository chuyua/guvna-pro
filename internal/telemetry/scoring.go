package telemetry

import (
	"context"
	"sort"
	"time"
)

// CandidateStat describes observed attempts, not a counterfactual routing policy.
// Stream and buffered attempts are separate because their latencies differ.
type CandidateStat struct {
	Provider                   string  `json:"provider"`
	Model                      string  `json:"model"`
	Stream                     bool    `json:"stream"`
	Attempts                   int     `json:"attempts"`
	Requests                   int     `json:"requests"`
	FirstAttempts              int     `json:"first_attempts"`
	FirstAttemptSuccesses      int     `json:"first_attempt_successes"`
	FirstAttemptSuccessRate    float64 `json:"first_attempt_success_rate"`
	FallbackRequests           int     `json:"fallback_requests"`
	FallbackRate               float64 `json:"fallback_rate"`
	P95LatencyMS               int64   `json:"p95_latency_ms"`
	Successes                  int     `json:"successes"`
	MeanLatencyMS              float64 `json:"mean_latency_ms"`
	SmoothedSuccessProbability float64 `json:"smoothed_success_probability"`
	ExpectedCostMS             float64 `json:"expected_cost_ms"`
}

type CandidateSnapshot struct {
	Since      time.Time       `json:"since"`
	Until      time.Time       `json:"until"`
	Rows       int             `json:"rows"`
	Truncated  bool            `json:"truncated"`
	Candidates []CandidateStat `json:"candidates"`
}

const scoringRowLimit = 100000

// CandidateStats reads the newest bounded sample in [since, until). Only rows
// with attempt metadata participate; legacy rows cannot reconstruct a walk.
// Requests crossing the window or row boundary are censored, so this snapshot
// is descriptive and cannot authorize replacing a live policy by itself.
func (t *Telemetry) CandidateStats(ctx context.Context, since, until time.Time) (CandidateSnapshot, error) {
	snapshot := CandidateSnapshot{Since: since, Until: until, Candidates: []CandidateStat{}}
	rows, err := t.db.QueryContext(ctx, `SELECT id, request_id, provider, model, stream,
		attempt, status, latency_ms, failure_class FROM requests
		WHERE request_id <> '' AND attempt > 0 AND failure_class <> 'canceled'
		AND julianday(ts) >= julianday(?) AND julianday(ts) < julianday(?)
		ORDER BY id DESC LIMIT ?`, since.UTC().Format(time.RFC3339Nano), until.UTC().Format(time.RFC3339Nano), scoringRowLimit+1)
	if err != nil {
		return snapshot, err
	}
	defer rows.Close()
	type candidateKey struct {
		provider, model string
		stream          bool
	}
	type requestKey struct {
		request string
		stream  bool
	}
	type candidateRequest struct {
		firstID int64
		lastID  int64
		firstOK bool
		first   bool
	}
	type aggregate struct {
		stat      CandidateStat
		latencies []int64
	}
	aggregates := make(map[candidateKey]*aggregate)
	requests := make(map[requestKey]map[candidateKey]*candidateRequest)
	for rows.Next() {
		if snapshot.Rows == scoringRowLimit {
			snapshot.Truncated = true
			break
		}
		var id, latency int64
		var request, provider, model, class string
		var stream bool
		var attempt, status int
		if err := rows.Scan(&id, &request, &provider, &model, &stream, &attempt, &status, &latency, &class); err != nil {
			return snapshot, err
		}
		snapshot.Rows++
		key := candidateKey{provider, model, stream}
		a := aggregates[key]
		if a == nil {
			a = &aggregate{stat: CandidateStat{Provider: provider, Model: model, Stream: stream}}
			aggregates[key] = a
		}
		a.stat.Attempts++
		if status >= 200 && status < 300 && class == "" {
			a.stat.Successes++
		}
		a.stat.MeanLatencyMS += float64(max(0, latency))
		a.latencies = append(a.latencies, max(0, latency))
		rk := requestKey{request, stream}
		if requests[rk] == nil {
			requests[rk] = make(map[candidateKey]*candidateRequest)
		}
		cr := requests[rk][key]
		if cr == nil {
			cr = &candidateRequest{firstID: id, lastID: id}
			requests[rk][key] = cr
		}
		cr.firstID = min(cr.firstID, id)
		cr.lastID = max(cr.lastID, id)
		if attempt == 1 {
			// Descending id traversal lets the earliest first attempt win when
			// a route contains the same candidate more than once.
			cr.first, cr.firstOK = true, status >= 200 && status < 300 && class == ""
		}
	}
	if err := rows.Err(); err != nil {
		return snapshot, err
	}
	for _, candidates := range requests {
		var last, second int64
		var lastKey candidateKey
		for key, cr := range candidates {
			if cr.lastID > last {
				second, last, lastKey = last, cr.lastID, key
			} else if cr.lastID > second {
				second = cr.lastID
			}
		}
		for key, cr := range candidates {
			a := aggregates[key]
			a.stat.Requests++
			if cr.first {
				a.stat.FirstAttempts++
				if cr.firstOK {
					a.stat.FirstAttemptSuccesses++
				}
			}
			other := last
			if key == lastKey {
				other = second
			}
			if other > cr.firstID {
				a.stat.FallbackRequests++
			}
		}
	}
	for _, a := range aggregates {
		if a.stat.FirstAttempts > 0 {
			a.stat.FirstAttemptSuccessRate = float64(a.stat.FirstAttemptSuccesses) / float64(a.stat.FirstAttempts)
		}
		a.stat.FallbackRate = float64(a.stat.FallbackRequests) / float64(a.stat.Requests)
		a.stat.MeanLatencyMS /= float64(a.stat.Attempts)
		a.stat.SmoothedSuccessProbability = float64(a.stat.Successes+1) / float64(a.stat.Attempts+2)
		// Millisecond measurements can round short local attempts to zero. A
		// 1ms cost floor prevents an instant failure from scoring free.
		a.stat.ExpectedCostMS = max(1, a.stat.MeanLatencyMS) / a.stat.SmoothedSuccessProbability
		sort.Slice(a.latencies, func(i, j int) bool { return a.latencies[i] < a.latencies[j] })
		a.stat.P95LatencyMS = a.latencies[(95*len(a.latencies)+99)/100-1]
		snapshot.Candidates = append(snapshot.Candidates, a.stat)
	}
	sort.Slice(snapshot.Candidates, func(i, j int) bool {
		a, b := snapshot.Candidates[i], snapshot.Candidates[j]
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		if a.Model != b.Model {
			return a.Model < b.Model
		}
		return !a.Stream && b.Stream
	})
	return snapshot, nil
}
