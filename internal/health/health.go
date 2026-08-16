// Package health tracks per-step provider health so chains can skip
// providers in cool-off. Steps are keyed by provider+model: one provider may
// host both a broken and a healthy model. Skipping is automatic — when the
// cool-off expires the step is tried again without admin action.
package health

import (
	"fmt"
	"sync"
	"time"
)

const (
	// FailureThreshold consecutive failures before a step is marked down.
	FailureThreshold = 3
	// CooldownBase is the initial cool-off after a step goes down.
	CooldownBase = 60 * time.Second
	// CooldownMax caps exponential cool-off growth.
	CooldownMax = 10 * time.Minute
)

// StepStatus is a snapshot of one step's health state.
type StepStatus struct {
	Provider string        `json:"provider"`
	Model    string        `json:"model"`
	Failures int           `json:"failures"`
	Down     bool          `json:"down"`
	DownFor  time.Duration `json:"down_for_ns"`
}

type stepKey struct{ provider, model string }

type state struct {
	failures  int
	downUntil time.Time
	lastAt    time.Time
	lastErr   string
}

// Tracker is safe for concurrent use.
type Tracker struct {
	mu     sync.Mutex
	states map[stepKey]*state
	now    func() time.Time
}

func New() *Tracker {
	return &Tracker{states: make(map[stepKey]*state), now: time.Now}
}

// Mark records a failure for a step. Cool-off grows exponentially with
// consecutive failures: 1m, 2m, 4m, ..., capped at CooldownMax.
func (t *Tracker) Mark(provider, model string, status int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.state(provider, model)
	st.failures++
	st.lastAt = t.now()
	if err != nil {
		st.lastErr = err.Error()
	} else {
		st.lastErr = fmt.Sprintf("status %d", status)
	}
	// Cool-off starts at CooldownBase when the step first goes down and
	// doubles for each consecutive failure beyond the threshold: 1m, 2m,
	// 4m, ... capped at CooldownMax.
	d := CooldownBase
	for i := FailureThreshold; i < st.failures && d < CooldownMax; i++ {
		d *= 2
	}
	if d > CooldownMax {
		d = CooldownMax
	}
	st.downUntil = st.lastAt.Add(d)
}

// MarkSuccess resets a step's failure state.
func (t *Tracker) MarkSuccess(provider, model string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.state(provider, model)
	st.failures = 0
	st.downUntil = time.Time{}
	st.lastErr = ""
}

// IsDown reports whether the step is in cool-off and should be skipped.
func (t *Tracker) IsDown(provider, model string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.state(provider, model)
	return st.failures >= FailureThreshold && t.now().Before(st.downUntil)
}

// Snapshot returns the health of every observed step, for the admin surface.
func (t *Tracker) Snapshot() []StepStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]StepStatus, 0, len(t.states))
	for k, st := range t.states {
		ss := StepStatus{Provider: k.provider, Model: k.model, Failures: st.failures}
		if st.failures >= FailureThreshold && t.now().Before(st.downUntil) {
			ss.Down = true
			ss.DownFor = st.downUntil.Sub(t.now())
		}
		out = append(out, ss)
	}
	return out
}

func (t *Tracker) state(provider, model string) *state {
	k := stepKey{provider, model}
	st, ok := t.states[k]
	if !ok {
		st = &state{}
		t.states[k] = st
	}
	return st
}
