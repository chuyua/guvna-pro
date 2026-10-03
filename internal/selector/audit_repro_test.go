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
