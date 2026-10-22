package keypool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/creamy-ghost/guvna/internal/config"
)

func TestClassFor(t *testing.T) {
	cases := []struct {
		status int
		err    error
		want   Class
	}{
		{401, nil, ClassAuth},
		{403, nil, ClassAuth},
		{429, nil, ClassRateLimit},
		{500, nil, ""},
		{502, nil, ""},
		{400, nil, ""},
		{404, nil, ""},
		{200, nil, ""},
		{0, errors.New("boom"), ""},       // local network failure: key neutral
		{0, context.DeadlineExceeded, ""}, // gateway timeout: key neutral
	}
	for _, c := range cases {
		if got := ClassFor(c.status, c.err); got != c.want {
			t.Errorf("ClassFor(%d, %v) = %q, want %q", c.status, c.err, got, c.want)
		}
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New("p", nil, "", config.QuarantineCfg{}); err == nil {
		t.Error("empty envs must error")
	}
	if _, err := New("p", []string{"K1"}, "bogus", config.QuarantineCfg{}); err == nil {
		t.Error("unknown strategy must error")
	}
	p, err := New("p", []string{"K1", "K2"}, "", config.QuarantineCfg{})
	if err != nil {
		t.Fatal(err)
	}
	if p.strat != StrategyRoundRobin {
		t.Errorf("default strategy = %q, want round_robin", p.strat)
	}
	if p.qc.Auth.Duration != config.DefaultQuarantine.Auth.Duration ||
		p.qc.RateLimit.Duration != config.DefaultQuarantine.RateLimit.Duration ||
		p.qc.Transient.Duration != config.DefaultQuarantine.Transient.Duration {
		t.Error("zero quarantine fields must fall back to defaults")
	}
}

func TestRoundRobinRotation(t *testing.T) {
	p, _ := New("p", []string{"K1", "K2", "K3"}, "round_robin", config.QuarantineCfg{})
	for i := 0; i < 3; i++ {
		if got := p.Pick(); got != "K"+string(rune('1'+i)) {
			t.Fatalf("pick %d = %s, want K%d", i, got, i+1)
		}
	}
	if got := p.Pick(); got != "K1" {
		t.Fatalf("pick after wrap = %s, want K1", got)
	}
}

func TestRoundRobinSkipsQuarantined(t *testing.T) {
	p, _ := New("p", []string{"K1", "K2", "K3"}, "round_robin", config.QuarantineCfg{})
	now := time.Now()
	p.now = func() time.Time { return now }
	p.Mark("K1", ClassAuth)
	p.Mark("K2", ClassAuth)
	if got := p.Pick(); got != "K3" {
		t.Fatalf("pick with K1/K2 down = %s, want K3", got)
	}
	if got := p.Pick(); got != "K3" {
		t.Fatalf("pick again = %s, want K3 (rrNext must not advance past quarantine)", got)
	}
}

func TestSequentialPrefersFirstHealthy(t *testing.T) {
	p, _ := New("p", []string{"K1", "K2", "K3"}, "sequential", config.QuarantineCfg{})
	now := time.Now()
	p.now = func() time.Time { return now }
	if got := p.Pick(); got != "K1" {
		t.Fatalf("first pick = %s, want K1", got)
	}
	p.Mark("K1", ClassRateLimit)
	if got := p.Pick(); got != "K2" {
		t.Fatalf("pick with K1 down = %s, want K2", got)
	}
}

func TestLeastUsedPicksOldest(t *testing.T) {
	p, _ := New("p", []string{"K1", "K2", "K3"}, "least_used", config.QuarantineCfg{})
	now := time.Now()
	p.now = func() time.Time { return now }
	p.Pick() // K1 lastAt = now
	now = now.Add(time.Second)
	if got := p.Pick(); got != "K2" {
		t.Fatalf("pick = %s, want K2 (K1 used most recently)", got)
	}
}

func TestAllQuarantinedReturnsUnavailable(t *testing.T) {
	p, _ := New("p", []string{"K1", "K2"}, "round_robin", config.QuarantineCfg{})
	now := time.Now()
	p.now = func() time.Time { return now }
	p.Mark("K1", ClassAuth) // down until now+24h
	now = now.Add(time.Minute)
	p.Mark("K2", ClassAuth) // down until now+24h (1 minute later than K1)
	if got := p.Pick(); got != "" {
		t.Fatalf("quarantined pick = %s, want unavailable", got)
	}
	now = p.keys[0].downUntil
	if got := p.Pick(); got != "K1" {
		t.Fatalf("pick at exact expiry = %s, want K1", got)
	}
}

func TestMarkDoublesAndCaps(t *testing.T) {
	p, _ := New("p", []string{"K"}, "round_robin", config.QuarantineCfg{})
	now := time.Now()
	p.now = func() time.Time { return now }
	p.Mark("K", ClassRateLimit)
	if got := p.keys[0].downUntil.Sub(now); got != 60*time.Second {
		t.Errorf("1st failure cool-off = %v, want 60s", got)
	}
	now = now.Add(time.Hour)
	p.Mark("K", ClassRateLimit)
	if got := p.keys[0].downUntil.Sub(now); got != 120*time.Second {
		t.Errorf("2nd failure cool-off = %v, want 120s", got)
	}
	now = now.Add(time.Hour)
	p.Mark("K", ClassRateLimit)
	if got := p.keys[0].downUntil.Sub(now); got != 240*time.Second {
		t.Errorf("3rd failure cool-off = %v, want 240s", got)
	}
	for i := 0; i < 15; i++ {
		now = now.Add(time.Hour)
		p.Mark("K", ClassRateLimit)
	}
	if got := p.keys[0].downUntil.Sub(now); got > config.DefaultQuarantine.Auth.Duration {
		t.Errorf("cool-off = %v, must cap at %v", got, config.DefaultQuarantine.Auth.Duration)
	}
}

func TestMarkSuccessResets(t *testing.T) {
	p, _ := New("p", []string{"K"}, "round_robin", config.QuarantineCfg{})
	now := time.Now()
	p.now = func() time.Time { return now }
	p.Mark("K", ClassAuth)
	if p.keys[0].healthy(now) {
		t.Fatal("key should be quarantined after mark")
	}
	p.MarkSuccess("K")
	if p.keys[0].healthy(now) || p.keys[0].failures != 1 {
		t.Fatal("late success must preserve a newer quarantine")
	}
	now = p.keys[0].downUntil
	if p.Pick() != "K" {
		t.Fatal("key must be selectable at expiry")
	}
	p.MarkSuccess("K")
	if !p.keys[0].healthy(now) {
		t.Error("key should be healthy after success")
	}
	if p.keys[0].failures != 0 || p.keys[0].lastClass != "" {
		t.Error("failures/lastClass must reset")
	}
}

func TestRecoveryAfterCoolOffExpires(t *testing.T) {
	p, _ := New("p", []string{"K"}, "round_robin", config.QuarantineCfg{})
	now := time.Now()
	p.now = func() time.Time { return now }
	p.Mark("K", ClassRateLimit)
	now = now.Add(61 * time.Second)
	if !p.keys[0].healthy(now) {
		t.Error("key must recover after cool-off expires")
	}
	if got := p.Pick(); got != "K" {
		t.Fatalf("pick after recovery = %s, want K", got)
	}
}

func TestRecordCounters(t *testing.T) {
	p, _ := New("p", []string{"K"}, "round_robin", config.QuarantineCfg{})
	p.Record("K", 10, 20)
	p.Record("K", 5, 7)
	p.Record("NOPE", 100, 100) // unknown key ignored
	ks := p.Keys()[0]
	if ks.Requests != 2 || ks.TokensIn != 15 || ks.TokensOut != 27 {
		t.Errorf("counters = req %d in %d out %d, want 2/15/27", ks.Requests, ks.TokensIn, ks.TokensOut)
	}
}

func TestKeysSnapshot(t *testing.T) {
	p, _ := New("p", []string{"K1", "K2"}, "round_robin", config.QuarantineCfg{})
	now := time.Now()
	p.now = func() time.Time { return now }
	ks := p.Keys()
	if ks[0].State != "healthy" || ks[0].Reason != "" {
		t.Errorf("healthy key state = %q/%q", ks[0].State, ks[0].Reason)
	}
	p.Mark("K1", ClassAuth)
	ks = p.Keys()
	if ks[0].State != "quarantined" || ks[0].Reason != "auth" {
		t.Errorf("quarantined key state = %q/%q, want quarantined/auth", ks[0].State, ks[0].Reason)
	}
	if ks[0].DownFor != 24*time.Hour {
		t.Errorf("down_for_ns = %v, want 24h", ks[0].DownFor)
	}
	if ks[0].Env != "K1" || ks[0].Index != 0 || ks[0].Provider != "p" {
		t.Errorf("identity fields wrong: %+v", ks[0])
	}
}

func TestMarkEmptyClassNoop(t *testing.T) {
	p, _ := New("p", []string{"K"}, "round_robin", config.QuarantineCfg{})
	p.Mark("K", "")
	if p.keys[0].failures != 0 {
		t.Error("empty class must not mark")
	}
}

func TestValidatedPoolPreservesRateLimitIsolation(t *testing.T) {
	cfg := &config.Config{Providers: []config.Provider{{
		Name: "p", Type: "openai", BaseURL: "https://provider.example", KeyEnvs: []string{"K1", "K2", "K1"},
	}}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("duplicate envs must be rejected before constructing a runtime pool")
	}
	cfg.Providers[0].KeyEnvs = []string{"K1", "K2"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	provider := cfg.Providers[0]
	clock := &leaseClock{}
	clock.tick.Store(time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC).UnixNano())
	p, err := NewWithClock(provider.Name, provider.AllKeyEnvs(), provider.Rotation, provider.Quarantine, clock.now)
	if err != nil {
		t.Fatal(err)
	}
	first := acquireLease(t, p)
	if first.Env() != "K1" || first.Failure(ClassFor(429, nil)) != ClassRateLimit {
		t.Fatal("first key did not enter the rate-limit quarantine")
	}
	for i := 0; i < 3; i++ {
		lease := acquireLease(t, p)
		if lease.Env() != "K2" {
			t.Fatalf("quarantined credential selected again: %q", lease.Env())
		}
		lease.Release()
	}
	if p.Available("K1") || p.Acquire(map[string]bool{"K2": true}) != nil {
		t.Fatal("rate-limited key remained available through a duplicate state")
	}
	status := p.Keys()
	if len(status) != 2 || status[0].State != "quarantined" || status[0].Reason != string(ClassRateLimit) || status[1].State != "healthy" {
		t.Fatalf("unexpected configured pool isolation: %+v", status)
	}
	clock.add(config.DefaultQuarantine.RateLimit.Duration)
	recovered := p.Acquire(map[string]bool{"K2": true})
	if recovered == nil || recovered.Env() != "K1" {
		t.Fatal("the unique credential did not recover at quarantine expiry")
	}
	recovered.Release()
}
