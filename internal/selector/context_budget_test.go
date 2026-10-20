package selector

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDecideContextSharedWaitHonorsCallerDeadline(t *testing.T) {
	cands := cacheCandidates("x")
	mu := &sync.Mutex{}
	call := &decideCall{done: make(chan struct{})}
	cache := &Cache{inflight: map[string]*decideCall{candidatesKey(cands): call}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var calls atomic.Int32
	start := time.Now()
	got, _, source := DecideContext(ctx, cands, func(context.Context) (map[string]int, error) {
		calls.Add(1)
		return nil, nil
	}, []string{"b", "a"}, time.Minute, mu, cache)
	if time.Since(start) > time.Second || calls.Load() != 0 || source != "decider_unavailable" || got[0].Step.Provider != "b" {
		t.Fatalf("shared wait exceeded budget: source=%s calls=%d order=%v", source, calls.Load(), got)
	}
	if cache.inflight[candidatesKey(cands)] != call {
		t.Fatal("a waiting caller removed the leader's inflight call")
	}
}

func TestDecideContextLeaderDeadlineDoesNotPoisonCache(t *testing.T) {
	cands := cacheCandidates("x")
	mu, cache := &sync.Mutex{}, &Cache{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	got, _, source := DecideContext(ctx, cands, func(ctx context.Context) (map[string]int, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, []string{"a", "b"}, time.Minute, mu, cache)
	if source != "decider_unavailable" || got[0].Step.Provider != "a" || len(cache.inflight) != 0 {
		t.Fatalf("expired leader state: source=%s inflight=%d", source, len(cache.inflight))
	}
	calls := 0
	got, _, source = DecideContext(context.Background(), cands, func(context.Context) (map[string]int, error) {
		calls++
		return map[string]int{"b/y": 0}, nil
	}, []string{"a", "b"}, time.Minute, mu, cache)
	if calls != 1 || source != "decider" || got[0].Step.Provider != "b" {
		t.Fatalf("caller deadline poisoned ranking cache: source=%s calls=%d", source, calls)
	}
}

func TestCanceledRefreshPreservesLastGoodAdviceWithoutRefreshingCache(t *testing.T) {
	cands := cacheCandidates("x")
	at := time.Now().Add(-2 * time.Minute)
	goodAt := time.Now()
	good := map[string]int{"b/y": 0}
	mu := &sync.Mutex{}
	cache := &Cache{key: candidatesKey(cands), order: good, at: at, goodOrder: good, goodAt: goodAt}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got, _, source := DecideContext(ctx, cands, func(ctx context.Context) (map[string]int, error) {
		cancel()
		return nil, ctx.Err()
	}, []string{"a", "b"}, time.Minute, mu, cache)
	if source != "decider_unavailable" || got[0].Step.Provider != "a" {
		t.Fatalf("canceled refresh used stale advice: source=%s order=%v", source, got)
	}
	if !cache.at.Equal(at) || !cache.goodAt.Equal(goodAt) || cache.stale || len(cache.inflight) != 0 {
		t.Fatalf("canceled refresh changed cache: %+v", cache)
	}
	got, _, source = DecideContext(context.Background(), cands, func(context.Context) (map[string]int, error) {
		return nil, context.DeadlineExceeded
	}, []string{"a", "b"}, time.Minute, mu, cache)
	if source != "decider_stale" || got[0].Step.Provider != "b" {
		t.Fatalf("canceled refresh discarded good advice: source=%s order=%v", source, got)
	}
}
