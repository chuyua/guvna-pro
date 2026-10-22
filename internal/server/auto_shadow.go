package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/creamy-ghost/guvna/internal/selector"
	"github.com/creamy-ghost/guvna/internal/telemetry"
)

const autoComparisonLimit = 512

type autoComparison struct {
	At             time.Time `json:"at"`
	RequestID      string    `json:"request_id"`
	Mode           string    `json:"mode"`
	DeciderSource  string    `json:"decider_source"`
	DeciderOrder   []string  `json:"decider_order"`
	TelemetryOrder []string  `json:"telemetry_order"`
	Disagrees      bool      `json:"disagrees"`
	Applied        bool      `json:"applied"`
	Reason         string    `json:"reason"`
}

type autoScoringState struct {
	mu       sync.Mutex
	snapshot telemetry.CandidateSnapshot
	loadedAt time.Time
	window   time.Duration
	err      error
	recent   [autoComparisonLimit]autoComparison
	next     int
	count    int
}

func (s *Server) autoScoringMode() string {
	switch mode := strings.TrimSpace(s.env("GUVNA_AUTO_ORDER_MODE")); mode {
	case "shadow", "telemetry":
		return mode
	default:
		return "decider"
	}
}

func (s *Server) autoScoringWindow() time.Duration {
	window, err := time.ParseDuration(s.env("GUVNA_AUTO_SCORE_WINDOW"))
	if err != nil || window <= 0 || window > 7*24*time.Hour {
		return time.Hour
	}
	return window
}

func (s *Server) autoScoringMinSamples() int {
	n, err := strconv.Atoi(s.env("GUVNA_AUTO_SCORE_MIN_SAMPLES"))
	if err != nil || n <= 0 {
		return 100
	}
	return n
}

// The cache bounds request-path queries to one per 15 seconds. Query failures
// are also cached and retain the decider order, rather than hammering SQLite.
func (s *Server) cachedAutoStats(ctx context.Context) (telemetry.CandidateSnapshot, error) {
	state := &s.autoScoring
	window := s.autoScoringWindow()
	state.mu.Lock()
	defer state.mu.Unlock()
	now := time.Now()
	if state.window == window && now.Sub(state.loadedAt) < 15*time.Second {
		return state.snapshot, state.err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	snapshot, err := s.tm.CandidateStats(queryCtx, now.Add(-window), now)
	state.snapshot, state.loadedAt, state.window, state.err = snapshot, now, window, err
	return snapshot, err
}

// telemetryCandidateOrder ranks sufficiently observed candidates by observed
// mean attempt latency / smoothed success probability. Unknown candidates
// retain their static relative order at the tail. It never changes eligibility.
func telemetryCandidateOrder(cands []selector.Candidate, order []string, snapshot telemetry.CandidateSnapshot, stream bool, minSamples int) ([]selector.Candidate, bool) {
	stats := make(map[string]telemetry.CandidateStat, len(snapshot.Candidates))
	for _, stat := range snapshot.Candidates {
		if stat.Stream == stream && stat.FirstAttempts >= minSamples {
			stats[stat.Provider+"/"+stat.Model] = stat
		}
	}
	result := selector.FallbackOrder(cands, order, nil)
	ready := !snapshot.Truncated && len(result) > 0
	for _, c := range result {
		if _, ok := stats[c.Step.Provider+"/"+c.Step.Model]; !ok {
			ready = false
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		a, aok := stats[result[i].Step.Provider+"/"+result[i].Step.Model]
		b, bok := stats[result[j].Step.Provider+"/"+result[j].Step.Model]
		if aok != bok {
			return aok
		}
		if !aok {
			return false
		}
		if a.ExpectedCostMS != b.ExpectedCostMS {
			return a.ExpectedCostMS < b.ExpectedCostMS
		}
		if a.FallbackRate != b.FallbackRate {
			return a.FallbackRate < b.FallbackRate
		}
		return false
	})
	return result, ready
}

func candidateNames(cands []selector.Candidate) []string {
	names := make([]string, len(cands))
	for i, c := range cands {
		names[i] = c.Step.Provider + "/" + c.Step.Model
	}
	return names
}

func (s *Server) applyAutoScoring(r *http.Request, decided []selector.Candidate, source string, stream bool) ([]selector.Candidate, string) {
	mode := s.autoScoringMode()
	if mode == "decider" || r.Context().Err() != nil {
		return decided, source
	}
	snapshot, err := s.cachedAutoStats(r.Context())
	shadow, ready := telemetryCandidateOrder(decided, s.providerOrder(), snapshot, stream, s.autoScoringMinSamples())
	comparison := autoComparison{At: time.Now(), RequestID: requestID(r), Mode: mode,
		DeciderSource: source, DeciderOrder: candidateNames(decided), TelemetryOrder: candidateNames(shadow), Reason: "shadow_only"}
	comparison.Disagrees = strings.Join(comparison.DeciderOrder, "\x00") != strings.Join(comparison.TelemetryOrder, "\x00")
	approved, _ := strconv.ParseBool(s.env("GUVNA_AUTO_TELEMETRY_APPROVED"))
	switch {
	case err != nil:
		comparison.Reason = "stats_unavailable"
	case mode == "telemetry" && !approved:
		comparison.Reason = "manual_comparison_required"
	case mode == "telemetry" && !ready:
		comparison.Reason = "insufficient_complete_samples"
	case mode == "telemetry":
		comparison.Applied, comparison.Reason = true, "operator_approved"
	}
	state := &s.autoScoring
	state.mu.Lock()
	state.recent[state.next] = comparison
	state.next = (state.next + 1) % autoComparisonLimit
	state.count = min(autoComparisonLimit, state.count+1)
	state.mu.Unlock()
	log.Printf("auto comparison: request_id=%s mode=%s disagrees=%v applied=%v reason=%s decider=%s telemetry=%s",
		comparison.RequestID, mode, comparison.Disagrees, comparison.Applied, comparison.Reason,
		strings.Join(comparison.DeciderOrder, ">"), strings.Join(comparison.TelemetryOrder, ">"))
	if comparison.Applied {
		return shadow, "telemetry"
	}
	return decided, source
}

// handleAutoScoring exposes observed metrics and the bounded restart-local
// comparison ring. It does not label unexecuted shadow choices as successes.
func (s *Server) handleAutoScoring(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	since, until := now.Add(-s.autoScoringWindow()), now
	for _, field := range []struct {
		name  string
		value *time.Time
	}{{"since", &since}, {"until", &until}} {
		if raw := r.URL.Query().Get(field.name); raw != "" {
			parsed, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				http.Error(w, fmt.Sprintf("invalid %s timestamp", field.name), http.StatusBadRequest)
				return
			}
			*field.value = parsed
		}
	}
	if !since.Before(until) || until.Sub(since) > 7*24*time.Hour {
		http.Error(w, "time window must be positive and at most 7 days", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	snapshot, err := s.tm.CandidateStats(ctx, since, until)
	if err != nil {
		http.Error(w, "candidate statistics unavailable", http.StatusServiceUnavailable)
		return
	}
	state := &s.autoScoring
	state.mu.Lock()
	comparisons := make([]autoComparison, 0, state.count)
	for i := 0; i < state.count; i++ {
		entry := state.recent[(state.next-1-i+autoComparisonLimit)%autoComparisonLimit]
		if !entry.At.Before(since) && entry.At.Before(until) {
			comparisons = append(comparisons, entry)
		}
	}
	state.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Mode                string                      `json:"mode"`
		MinSamples          int                         `json:"min_samples"`
		Metrics             telemetry.CandidateSnapshot `json:"metrics"`
		Comparisons         []autoComparison            `json:"comparisons"`
		ComparisonRetention string                      `json:"comparison_retention"`
	}{s.autoScoringMode(), s.autoScoringMinSamples(), snapshot, comparisons, "latest 512, process lifetime"})
}
