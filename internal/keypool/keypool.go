// Package keypool manages a provider's pool of API keys: selection strategy,
// class-based quarantine and per-key usage counters. Keys are identified by
// their env var name; values are resolved lazily by the caller so env changes
// take effect without a restart.
//
// Selection skips quarantined keys; an exhausted pool lets the caller fail over.
package keypool

import (
	"fmt"
	"sync"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
	"github.com/creamy-ghost/guvna/internal/outcome"
)

// Failure classes mirror upstream status semantics.
type Class string

const (
	ClassAuth      Class = "auth"       // 401/403: key dead or quota gone
	ClassRateLimit Class = "rate_limit" // 429: temporarily rate-limited
	ClassTransient Class = "transient"  // 5xx / upstream budget failures
)

// Strategy selects the next key among healthy ones.
type Strategy string

const (
	StrategyRoundRobin Strategy = "round_robin"
	StrategyLeastUsed  Strategy = "least_used"
	StrategySequential Strategy = "sequential"
)

// KeyStatus is one key's observable state for the admin surface.
type KeyStatus struct {
	Provider  string        `json:"provider"`
	Index     int           `json:"index"`
	Env       string        `json:"env"`
	State     string        `json:"state"`  // healthy | quarantined
	Reason    string        `json:"reason"` // auth | rate_limit | transient | network | ""
	Failures  int           `json:"failures"`
	DownFor   time.Duration `json:"down_for_ns"`
	Requests  int64         `json:"requests"`
	TokensIn  int64         `json:"tokens_in"`
	TokensOut int64         `json:"tokens_out"`
}

type keyState struct {
	env        string
	failures   int
	downUntil  time.Time
	lastAt     time.Time
	lastClass  Class
	generation uint64
	// failureGeneration is the admission epoch that established the current
	// quarantine. Only an auth result from that cohort may upgrade its class.
	failureGeneration uint64
	requests          int64
	tokensIn          int64
	tokensOut         int64
}

func (k *keyState) healthy(now time.Time) bool {
	return k.failures == 0 || !now.Before(k.downUntil)
}

// Pool is safe for concurrent use.
type Pool struct {
	name   string
	keys   []*keyState
	strat  Strategy
	qc     config.QuarantineCfg
	now    func() time.Time
	rrNext int
	mu     sync.Mutex
}

// New builds a pool for the provider. Strategy "" and zero quarantine fields
// fall back to round_robin and config.DefaultQuarantine.
func New(provider string, envs []string, strategy string, qc config.QuarantineCfg) (*Pool, error) {
	return NewWithClock(provider, envs, strategy, qc, time.Now)
}

// NewWithClock allows deterministic evaluation of quarantine expiry. The clock
// must be safe for concurrent use.
func NewWithClock(provider string, envs []string, strategy string, qc config.QuarantineCfg, now func() time.Time) (*Pool, error) {
	if len(envs) == 0 {
		return nil, fmt.Errorf("provider %q: empty key pool", provider)
	}
	s := Strategy(strategy)
	switch s {
	case "", StrategyRoundRobin:
		s = StrategyRoundRobin
	case StrategyLeastUsed, StrategySequential:
	default:
		return nil, fmt.Errorf("provider %q: unknown rotation %q", provider, strategy)
	}
	if qc.Auth.Duration == 0 {
		qc.Auth = config.DefaultQuarantine.Auth
	}
	if qc.RateLimit.Duration == 0 {
		qc.RateLimit = config.DefaultQuarantine.RateLimit
	}
	if qc.Transient.Duration == 0 {
		qc.Transient = config.DefaultQuarantine.Transient
	}
	keys := make([]*keyState, 0, len(envs))
	for _, e := range envs {
		keys = append(keys, &keyState{env: e})
	}
	return &Pool{name: provider, keys: keys, strat: s, qc: qc, now: now}, nil
}

// Pick returns the env var name of the key to use for the next attempt.
// It returns "" if every key is still quarantined.
func (p *Pool) Pick() string {
	return p.PickExcluding(nil)
}

// PickExcluding selects with the configured strategy while skipping credentials
// already used by this execution step. It never manufactures key failures.
func (p *Pool) PickExcluding(excluded map[string]bool) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.pick(excluded, p.now())
	if k == nil {
		return ""
	}
	return k.env
}

// pick requires p.mu and preserves the selection strategy for both the legacy
// string API and generation-bound acquisitions.
func (p *Pool) pick(excluded map[string]bool, now time.Time) *keyState {
	best := -1
	switch p.strat {
	case StrategySequential:
		for i, k := range p.keys {
			if !excluded[k.env] && k.healthy(now) {
				best = i
				break
			}
		}
	case StrategyLeastUsed:
		for i, k := range p.keys {
			if excluded[k.env] || !k.healthy(now) {
				continue
			}
			if best == -1 || k.lastAt.Before(p.keys[best].lastAt) {
				best = i
			}
		}
	default: // round_robin
		n := len(p.keys)
		for i := 0; i < n; i++ {
			idx := (p.rrNext + i) % n
			if !excluded[p.keys[idx].env] && p.keys[idx].healthy(now) {
				best = idx
				break
			}
		}
	}
	if best == -1 {
		return nil
	}
	p.keys[best].lastAt = now
	if p.strat == StrategyRoundRobin {
		p.rrNext = (best + 1) % len(p.keys)
	}
	return p.keys[best]
}

// Lease binds one real attempt to the selected key's admission generation.
// It does not make healthy traffic exclusive. Complete it exactly once with
// Failure, Success or Release; stale completions cannot alter a later window.
type Lease struct {
	pool       *Pool
	key        *keyState
	generation uint64
	done       bool // guarded by pool.mu
}

// Acquire atomically selects a key and captures its generation. It returns nil
// when every key is quarantined or excluded. Call Available after a backoff.
func (p *Pool) Acquire(excluded map[string]bool) *Lease {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.pick(excluded, p.now())
	if k == nil {
		return nil
	}
	return &Lease{pool: p, key: k, generation: k.generation}
}

func (l *Lease) Env() string {
	if l == nil {
		return ""
	}
	return l.key.env
}

// Available also checks generation: an expired quarantine does not revive an
// attempt admitted before the failure that established it.
func (l *Lease) Available() bool {
	if l == nil {
		return false
	}
	p := l.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	return !l.done && l.generation == l.key.generation && l.key.healthy(p.now())
}

// Failure completes an attempt. Concurrent auth evidence may upgrade the
// current non-auth quarantine only if it came from the cohort that created
// that exact window; it cannot extend another auth window or a recovered key.
func (l *Lease) Failure(class Class) Class {
	if l == nil {
		return ""
	}
	p := l.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if l.done {
		return ""
	}
	l.done = true
	if class == "" {
		return ""
	}
	k, now := l.key, p.now()
	if l.generation != k.generation {
		if l.generation == k.failureGeneration && !k.healthy(now) {
			return p.upgradeAuth(k, class, now)
		}
		return ""
	}
	return p.mark(k, class, now)
}

// Success resets only an available key in this attempt's generation. A stale
// response cannot reset a quarantine, even after that quarantine has expired.
func (l *Lease) Success() {
	if l == nil {
		return
	}
	p := l.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if l.done {
		return
	}
	l.done = true
	if l.generation == l.key.generation && l.key.healthy(p.now()) {
		reset(l.key)
	}
}

// Release abandons the attempt without changing health or usage counters.
func (l *Lease) Release() {
	if l == nil {
		return
	}
	p := l.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	l.done = true
}

// Available rechecks a selected key after a cancellable backoff. Another
// request may have quarantined it while this attempt was waiting.
func (p *Pool) Available(env string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.byEnv(env)
	return k != nil && k.healthy(p.now())
}

// ClassFor maps an upstream status or error to a failure class using the
// shared outcome classifier. Returns "" for non-quarantinable outcomes
// (caller errors, model incompatibility, local network failures and
// cancellation): those must never quarantine a healthy key. 5xx keeps the
// documented transient quarantine; 401/403 quarantine as auth.
func ClassFor(status int, err error) Class {
	return Class(outcome.KeyClass(outcome.Classify(status, err)))
}

// Mark is the legacy, unbound failure API. Repeated results within an active
// quarantine do not extend it, except a single auth upgrade. Execution paths
// should use Acquire/Lease to also reject old results across expiry/recovery.
func (p *Pool) Mark(env string, class Class) Class {
	if class == "" {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.byEnv(env)
	if k == nil {
		return ""
	}
	return p.mark(k, class, p.now())
}

// mark requires p.mu. One accepted failure establishes a new generation.
func (p *Pool) mark(k *keyState, class Class, now time.Time) Class {
	if !k.healthy(now) {
		return p.upgradeAuth(k, class, now)
	}
	d := p.cooldown(class, k.failures)
	k.failureGeneration = k.generation
	k.generation++
	k.failures++
	k.lastAt = now
	k.lastClass = class
	k.downUntil = k.lastAt.Add(d)
	return class
}

// upgradeAuth replaces the current outage's classification, rather than
// counting a second outage. Repeated auth evidence cannot stretch the window.
func (p *Pool) upgradeAuth(k *keyState, class Class, now time.Time) Class {
	if class != ClassAuth || k.lastClass == ClassAuth {
		return ""
	}
	k.lastClass = ClassAuth
	k.lastAt = now
	k.generation++
	k.downUntil = maxTime(k.downUntil, now.Add(p.cooldown(class, max(0, k.failures-1))))
	return class
}

func (p *Pool) cooldown(class Class, failures int) time.Duration {
	d := p.durationFor(class)
	cap := config.DefaultQuarantine.Auth.Duration
	for i := 0; i < failures && d < cap; i++ {
		if d > cap/2 {
			return cap
		}
		d *= 2
	}
	return min(d, cap)
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func reset(k *keyState) {
	if k.failures != 0 || k.lastClass != "" || !k.downUntil.IsZero() {
		k.generation++
	}
	k.failures = 0
	k.downUntil = time.Time{}
	k.lastClass = ""
}

// MarkSuccess resets a recovered key's failure state. A response arriving while
// the key is quarantined belongs to an older in-flight attempt: selection no
// longer admits this key, so that old success must not erase the quarantine.
func (p *Pool) MarkSuccess(env string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if k := p.byEnv(env); k != nil && k.healthy(p.now()) {
		reset(k)
	}
}

// Record adds usage counters for the key.
func (p *Pool) Record(env string, in, out int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if k := p.byEnv(env); k != nil {
		k.requests++
		k.tokensIn += in
		k.tokensOut += out
	}
}

// Keys returns the current state of every key, for the admin surface.
func (p *Pool) Keys() []KeyStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	out := make([]KeyStatus, 0, len(p.keys))
	for i, k := range p.keys {
		ks := KeyStatus{
			Provider:  p.name,
			Index:     i,
			Env:       k.env,
			Failures:  k.failures,
			Requests:  k.requests,
			TokensIn:  k.tokensIn,
			TokensOut: k.tokensOut,
		}
		if !k.healthy(now) {
			ks.State = "quarantined"
			ks.Reason = string(k.lastClass)
			ks.DownFor = k.downUntil.Sub(now)
		} else {
			ks.State = "healthy"
		}
		out = append(out, ks)
	}
	return out
}

func (p *Pool) byEnv(env string) *keyState {
	for _, k := range p.keys {
		if k.env == env {
			return k
		}
	}
	return nil
}

func (p *Pool) durationFor(class Class) time.Duration {
	switch class {
	case ClassAuth:
		return p.qc.Auth.Duration
	case ClassRateLimit:
		return p.qc.RateLimit.Duration
	default:
		return p.qc.Transient.Duration
	}
}
