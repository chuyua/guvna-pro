package selector

import (
	"context"
	"github.com/creamy-ghost/guvna/internal/router"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAuditConcurrentDecider(t *testing.T) {
	const n = 12
	var calls atomic.Int32
	entered := make(chan struct{}, n)
	release := make(chan struct{})
	start := make(chan struct{})
	decider := func(context.Context) (map[string]int, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return map[string]int{"a/x": 0, "b/y": 1}, nil
	}
	cands := []Candidate{{Chain: "x", Step: router.Step{Provider: "a", Model: "x"}}, {Chain: "y", Step: router.Step{Provider: "b", Model: "y"}}}
	mu := &sync.Mutex{}
	cache := &Cache{}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; Decide(cands, decider, nil, time.Minute, mu, cache) }()
	}
	close(start)
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
wait:
	for i := 0; i < n; i++ {
		select {
		case <-entered:
		case <-timer.C:
			break wait
		}
	}
	close(release)
	wg.Wait()
	t.Logf("same cold cache, concurrent requests=%d decider calls=%d", n, calls.Load())
	if calls.Load() != 1 {
		t.Fatalf("expected at most one decider call per interval, got %d", calls.Load())
	}
}

// A waiter must not block on a slow leader past the client's patience: the
// background refresh's decider call can run to the full 90s header timeout,
// while the request path has a 10s wait budget. After the share wait the
// request takes the static fallback instead of hanging (observed on prod:
// 60s client disconnects with every candidate still queued).
func TestAuditShareWaitFallsBackOnSlowLeader(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	decider := func(context.Context) (map[string]int, error) {
		entered <- struct{}{}
		<-release
		return map[string]int{"a/x": 0, "b/y": 1}, nil
	}
	cands := []Candidate{{Chain: "x", Step: router.Step{Provider: "a", Model: "x"}}, {Chain: "y", Step: router.Step{Provider: "b", Model: "y"}}}
	mu := &sync.Mutex{}
	cache := &Cache{}
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		Decide(cands, decider, nil, time.Minute, mu, cache)
	}()
	<-entered
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		_, _, source := Decide(cands, decider, nil, time.Minute, mu, cache)
		// Decide maps any decideOrder error to the static fallback source.
		if source != "decider_unavailable" {
			t.Errorf("waiter source = %q, want decider_unavailable (fallback on share-wait expiry)", source)
		}
	}()
	select {
	case <-waiterDone:
	case <-time.After(15 * time.Second):
		t.Fatal("waiter blocked past the share wait budget")
	}
	close(release)
	<-leaderDone
}
