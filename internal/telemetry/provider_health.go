package telemetry

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// ProviderHealthStat describes observed request-path availability. Network
// failures may be local, and auth/request errors are not provider outages.
type ProviderHealthStat struct {
	Provider           string    `json:"provider"`
	Stream             bool      `json:"stream"`
	Attempts           int       `json:"attempts"`
	Successes          int       `json:"successes"`
	UpstreamFailures   int       `json:"upstream_failures"`
	NetworkFailures    int       `json:"network_failures"`
	AuthFailures       int       `json:"auth_failures"`
	RateLimited        int       `json:"rate_limited"`
	RequestErrors      int       `json:"request_errors"`
	Canceled           int       `json:"canceled"`
	OtherFailures      int       `json:"other_failures"`
	HealthSamples      int       `json:"health_samples"`
	ServiceSuccessRate *float64  `json:"service_success_rate"`
	P95LatencyMS       int64     `json:"p95_latency_ms"`
	LastAttemptAt      time.Time `json:"last_attempt_at"`
}

type ProviderHealthSnapshot struct {
	Since     time.Time            `json:"since"`
	Until     time.Time            `json:"until"`
	Rows      int                  `json:"rows"`
	Truncated bool                 `json:"truncated"`
	Providers []ProviderHealthStat `json:"providers"`
}

// ProviderHealthStats groups existing attempts, without probing any upstream.
// The query scans at most the newest scoringRowLimit+1 stored rows near the
// window. SQLite rounds RFC3339 fractions to milliseconds, so the coarse SQL
// window is widened and Go enforces exact [since,until) nanosecond bounds.
// Reaching the scan limit marks the snapshot truncated conservatively, even
// when some boundary rows were excluded by that exact check.
func (t *Telemetry) ProviderHealthStats(ctx context.Context, since, until time.Time) (ProviderHealthSnapshot, error) {
	snapshot := ProviderHealthSnapshot{Since: since, Until: until, Providers: []ProviderHealthStat{}}
	rows, err := t.db.QueryContext(ctx, `SELECT ts, provider, stream, status,
		latency_ms, failure_class FROM requests
		WHERE request_id <> '' AND attempt > 0
		AND julianday(ts) >= julianday(?) AND julianday(ts) <= julianday(?)
		ORDER BY id DESC LIMIT ?`, since.Add(-time.Millisecond).UTC().Format(time.RFC3339Nano),
		until.Add(time.Millisecond).UTC().Format(time.RFC3339Nano), scoringRowLimit+1)
	if err != nil {
		return snapshot, err
	}
	defer rows.Close()
	type key struct {
		provider string
		stream   bool
	}
	type aggregate struct {
		stat      ProviderHealthStat
		latencies []int64
	}
	groups := make(map[key]*aggregate)
	scanned := 0
	for rows.Next() {
		if scanned == scoringRowLimit {
			snapshot.Truncated = true
			break
		}
		scanned++
		var k key
		var raw, class string
		var status int
		var latency int64
		if err := rows.Scan(&raw, &k.provider, &k.stream, &status, &latency, &class); err != nil {
			return snapshot, err
		}
		at, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return snapshot, fmt.Errorf("parse provider attempt timestamp: %w", err)
		}
		if at.Before(since) || !at.Before(until) {
			continue
		}
		snapshot.Rows++
		a := groups[k]
		if a == nil {
			a = &aggregate{stat: ProviderHealthStat{Provider: k.provider, Stream: k.stream}}
			groups[k] = a
		}
		a.stat.Attempts++
		if at.After(a.stat.LastAttemptAt) {
			a.stat.LastAttemptAt = at
		}
		switch {
		case status >= 200 && status < 300 && class == "":
			a.stat.Successes++
		case class == "upstream_transient":
			a.stat.UpstreamFailures++
		case class == "local_network":
			a.stat.NetworkFailures++
		case class == "auth":
			a.stat.AuthFailures++
		case class == "rate_limit":
			a.stat.RateLimited++
		case class == "caller_error" || class == "model_incompatible":
			a.stat.RequestErrors++
		case class == "canceled":
			a.stat.Canceled++
		default:
			a.stat.OtherFailures++
		}
		if class != "canceled" {
			a.latencies = append(a.latencies, max(0, latency))
		}
	}
	if err := rows.Err(); err != nil {
		return snapshot, err
	}
	for _, a := range groups {
		a.stat.HealthSamples = a.stat.Successes + a.stat.UpstreamFailures + a.stat.NetworkFailures + a.stat.RateLimited
		if a.stat.HealthSamples > 0 {
			rate := float64(a.stat.Successes) / float64(a.stat.HealthSamples)
			a.stat.ServiceSuccessRate = &rate
		}
		if len(a.latencies) > 0 {
			sort.Slice(a.latencies, func(i, j int) bool { return a.latencies[i] < a.latencies[j] })
			a.stat.P95LatencyMS = a.latencies[(95*len(a.latencies)+99)/100-1]
		}
		snapshot.Providers = append(snapshot.Providers, a.stat)
	}
	sort.Slice(snapshot.Providers, func(i, j int) bool {
		a, b := snapshot.Providers[i], snapshot.Providers[j]
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		return !a.Stream && b.Stream
	})
	return snapshot, nil
}
