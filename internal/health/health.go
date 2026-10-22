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
	// PermanentCooldown is the fixed cool-off for an attributed incompatibility.
	PermanentCooldown = 5 * time.Minute
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
	failures   int
	downUntil  time.Time
	lastAt     time.Time
	lastErr    string
	generation uint64
	probing    bool
}

// Tracker is safe for concurrent use.
type Tracker struct {
	mu     sync.Mutex
	states map[stepKey]*state
	now    func() time.Time
}

func New() *Tracker {
	return NewWithClock(time.Now)
}

// NewWithClock builds a tracker with an injectable clock for deterministic
// cooldown evaluation. The clock must be safe for concurrent use.
func NewWithClock(now func() time.Time) *Tracker {
	return &Tracker{states: make(map[stepKey]*state), now: now}
}

// Mark records a failure for a step. Cool-off grows exponentially with
// consecutive failures: 1m, 2m, 4m, ..., capped at CooldownMax.
//
// An attributed 400, 404 or 422 means the request is incompatible with this model — bad
// request shape for the model, or the model does not exist on the provider.
// Such errors are permanent at the model level and do not heal on retry, so
// one occurrence is enough to mark the step down; the cool-off is fixed
// rather than exponential because there is nothing to back off from — after
// PermanentCooldown the step is probed once to see if the model was added or
// restored, and re-marked immediately if not.
func (t *Tracker) Mark(provider, model string, status int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.state(provider, model)
	t.mark(st, status, err)
}

func (t *Tracker) mark(st *state, status int, err error) {
	// A failure advances the admission epoch. Other requests admitted before
	// this result describe the same outage and cannot escalate it again or
	// erase it with a delayed success. Fresh serial admissions still count.
	st.generation++
	st.probing = false
	st.failures++
	st.lastAt = t.now()
	if err != nil {
		st.lastErr = err.Error()
	} else {
		st.lastErr = fmt.Sprintf("status %d", status)
	}
	if status == 400 || status == 404 || status == 422 {
		if st.failures < FailureThreshold {
			st.failures = FailureThreshold
		}
		st.downUntil = st.lastAt.Add(PermanentCooldown)
		return
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
	st.generation++
	st.probing = false
	reset(st)
}

func reset(st *state) {
	st.failures = 0
	st.downUntil = time.Time{}
	st.lastErr = ""
}

// IsDown reports whether the step is in cool-off and should be skipped.
func (t *Tracker) IsDown(provider, model string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.states[stepKey{provider, model}]
	return st != nil && st.failures >= FailureThreshold && (st.probing || t.now().Before(st.downUntil))
}

// Permit owns an execution admission. Release abandons it without recording a
// failure (for example on cancellation or when no key is available).
type Permit struct {
	tracker    *Tracker
	key        stepKey
	generation uint64
	probe      bool
	done       bool // guarded by tracker.mu
}

// DeferredFailure releases an execution permit while preserving its admission
// generation. Confirm may later attribute an incompatibility after another
// candidate has served a usable result; stale or repeated confirmations do
// nothing. It never holds a half-open recovery probe open while the walk moves on.
type DeferredFailure struct {
	tracker    *Tracker
	key        stepKey
	generation uint64
	status     int
	err        error
	done       bool // guarded by tracker.mu
}

func (p *Permit) DeferFailure(status int, err error) *DeferredFailure {
	if p == nil {
		return nil
	}
	t := p.tracker
	t.mu.Lock()
	defer t.mu.Unlock()
	if p.done {
		return nil
	}
	p.done = true
	st := t.states[p.key]
	if st == nil || st.generation != p.generation {
		return nil
	}
	if p.probe {
		st.probing = false
	}
	return &DeferredFailure{tracker: t, key: p.key, generation: p.generation, status: status, err: err}
}

func (d *DeferredFailure) Confirm() {
	if d == nil {
		return
	}
	t := d.tracker
	t.mu.Lock()
	defer t.mu.Unlock()
	if d.done {
		return
	}
	d.done = true
	st := t.states[d.key]
	if st != nil && st.generation == d.generation {
		t.mark(st, d.status, d.err)
	}
}

// TryAcquire admits healthy traffic freely, but only one probe after cool-off.
// Callers must release every permit even if no upstream request is made.
func (t *Tracker) TryAcquire(provider, model string) (*Permit, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.state(provider, model)
	probe := st.failures >= FailureThreshold
	if probe {
		if st.probing || t.now().Before(st.downUntil) {
			return nil, false
		}
		st.generation++
		st.probing = true
	}
	return &Permit{tracker: t, key: stepKey{provider, model}, generation: st.generation, probe: probe}, true
}

func (p *Permit) Release()                      { p.finish(false, false, 0, nil) }
func (p *Permit) Success()                      { p.finish(true, true, 0, nil) }
func (p *Permit) Failure(status int, err error) { p.finish(true, false, status, err) }

func (p *Permit) finish(record, success bool, status int, err error) {
	if p == nil {
		return
	}
	t := p.tracker
	t.mu.Lock()
	defer t.mu.Unlock()
	if p.done {
		return
	}
	p.done = true
	st := t.states[p.key]
	if st.generation != p.generation {
		return
	}
	if p.probe {
		st.probing = false
	}
	if !record {
		return
	}
	if success {
		// An older healthy request cannot erase a cooldown established while
		// it was in flight. Only its admitted recovery probe can reset it.
		if !p.probe && st.failures >= FailureThreshold {
			return
		}
		reset(st)
		// A completed newer success also fences deferred blame and older
		// results, even when it did not own a half-open probe.
		st.generation++
	} else {
		t.mark(st, status, err)
	}
}

// Snapshot returns the health of every observed step, for the admin surface.
func (t *Tracker) Snapshot() []StepStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]StepStatus, 0, len(t.states))
	for k, st := range t.states {
		ss := StepStatus{Provider: k.provider, Model: k.model, Failures: st.failures}
		if st.failures >= FailureThreshold && (st.probing || t.now().Before(st.downUntil)) {
			ss.Down = true
			if t.now().Before(st.downUntil) {
				ss.DownFor = st.downUntil.Sub(t.now())
			}
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
