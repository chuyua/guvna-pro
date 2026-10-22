package server

import (
	"context"
	"net/http"
	"time"

	"github.com/creamy-ghost/guvna/internal/outcome"
	"github.com/creamy-ghost/guvna/internal/router"
	"github.com/creamy-ghost/guvna/internal/telemetry"
)

type autoObservationKey struct{}

// The Auto walk, retry loop and complete relay share the handler goroutine.
// Keep a request-local summary separately from each candidate's attempt trace.
type autoObservation struct {
	event      telemetry.AutoRequestEvent
	candidates map[[2]string]struct{}
	pending    bool
}

func (s *Server) startAutoObservation(r *http.Request, started time.Time) (*http.Request, *autoObservation) {
	observation := &autoObservation{
		event: telemetry.AutoRequestEvent{
			RequestID: requestID(r), StartedAt: started, Mode: s.autoScoringMode(), Policy: "none",
		},
		candidates: make(map[[2]string]struct{}),
	}
	return r.WithContext(context.WithValue(r.Context(), autoObservationKey{}, observation)), observation
}

func autoObservationFor(r *http.Request) *autoObservation {
	observation, _ := r.Context().Value(autoObservationKey{}).(*autoObservation)
	return observation
}

func (o *autoObservation) selected(originalSource, appliedSource string) {
	o.event.DeciderSource = originalSource
	switch appliedSource {
	case "telemetry":
		o.event.Policy = "telemetry"
	case "decider", "decider_cached", "decider_stale", "decider_shared":
		o.event.Policy = "decider"
	default: // single_candidate and decider_unavailable use static priority.
		o.event.Policy = "static"
	}
}

// Count only an actual adaptor call. A new call cannot inherit an earlier
// candidate's terminal outcome when its own relay ends without a result.
func beginAutoAttempt(r *http.Request, step router.Step) {
	if o := autoObservationFor(r); o != nil {
		o.event.Attempts++
		o.candidates[[2]string{step.Provider, step.Model}] = struct{}{}
		o.pending = true
		o.event.Success, o.event.Canceled = false, false
		o.event.Status, o.event.FailureClass = 0, ""
	}
}

// Called only after recordAttempt's once-only terminal guard accepts a result.
// Success here means the complete body or stream was validated and delivered.
func recordAutoAttempt(r *http.Request, status int, err error) {
	if o := autoObservationFor(r); o != nil {
		o.pending = false
		o.event.Status = status
		o.event.FailureClass = string(outcome.Classify(status, err))
		o.event.Success = status >= 200 && status < 300 && o.event.FailureClass == ""
	}
}

func (o *autoObservation) fail(status int, class string) {
	o.pending = false
	o.event.Success, o.event.Canceled = false, false
	o.event.Status, o.event.FailureClass = status, class
}

func (o *autoObservation) cancel() {
	o.pending = false
	o.event.Success, o.event.Canceled = false, true
	o.event.Status, o.event.FailureClass = 0, string(outcome.Canceled)
}

func (s *Server) finishAutoObservation(r *http.Request, o *autoObservation) {
	if r.Context().Err() != nil || o.pending {
		// A relay which produced neither validated completion nor a recorded
		// upstream error stopped on the downstream side. It is neutral for health.
		o.cancel()
	}
	o.event.Candidates = len(o.candidates)
	o.event.FinishedAt = time.Now()
	o.event.LatencyMS = max(0, o.event.FinishedAt.Sub(o.event.StartedAt).Milliseconds())
	s.tm.RecordAutoRequest(o.event)
}
